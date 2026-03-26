//! 设备管理领域服务
//!
//! 职责：
//! - 多设备管理（同一用户的多个设备在线）
//! - Token 版本校验与强制下线
//! - 设备优先级管理（用于推送策略）
//! - 链接质量排序（选择最优设备）

use anyhow::Result;
use std::sync::Arc;
use tracing::{info, warn};

use crate::domain::model::{ConnectionQualityRecord, ConnectionRecord};
use crate::domain::repository::ConversationRepository;
use crate::domain::value_object::{ConnectionId, UserId};

/// 设备管理领域服务（泛型仓储，避免 `dyn` 异步 trait）
pub struct DeviceManagerService<R: ConversationRepository + Send + Sync> {
    repository: Arc<R>,
}

impl<R: ConversationRepository + Send + Sync> DeviceManagerService<R> {
    pub fn new(repository: Arc<R>) -> Self {
        Self { repository }
    }

    /// 获取用户的所有在线设备会话
    pub async fn get_user_devices(&self, user_id: &str) -> Result<Vec<ConnectionRecord>> {
        let uid = UserId::new(user_id.to_string()).map_err(|e| anyhow::anyhow!(e))?;
        let sessions = self.repository.get_user_connections(&uid).await?;
        let records: Vec<ConnectionRecord> = sessions
            .into_iter()
            .map(|s| ConnectionRecord {
                conversation_id: s.id().as_str().to_string(),
                user_id: user_id.to_string(),
                device_id: s.device_id().as_str().to_string(),
                device_platform: s.device_platform().to_string(),
                server_id: s.server_id().to_string(),
                gateway_id: s.gateway_id().to_string(),
                last_seen: s.last_heartbeat_at(),
                device_priority: s.device_priority().as_i32(),
                token_version: s.token_version().value(),
                connection_quality: s.connection_quality().map(|q| ConnectionQualityRecord {
                    rtt_ms: q.rtt_ms(),
                    packet_loss_rate: q.packet_loss_rate(),
                    last_measure_ts: (q.last_measure_ts().timestamp() * 1000)
                        + (q.last_measure_ts().timestamp_subsec_millis() as i64),
                    network_type: q.network_type().as_str().to_string(),
                    signal_strength: q.signal_strength().unwrap_or_default(),
                }),
            })
            .collect();
        Ok(records)
    }

    /// Token 版本校验：检查是否需要踢出旧版本设备
    ///
    /// 返回：需要踢出的会话 ID 列表
    pub async fn check_token_version(
        &self,
        user_id: &str,
        new_token_version: i64,
    ) -> Result<Vec<String>> {
        let uid = UserId::new(user_id.to_string()).map_err(|e| anyhow::anyhow!(e))?;
        let sessions = self.repository.get_user_connections(&uid).await?;

        let mut to_kick = Vec::new();
        for session in sessions {
            let old_version = session.token_version().value();
            if old_version > 0 && old_version < new_token_version {
                info!(
                    user_id = %user_id,
                    conversation_id = %session.id().as_str(),
                    old_version = old_version,
                    new_version = new_token_version,
                    "Token version mismatch, will kick old session"
                );
                to_kick.push(session.id().as_str().to_string());
            }
        }
        Ok(to_kick)
    }

    /// 按设备优先级排序设备列表
    ///
    /// 优先级顺序：Critical(4) > High(3) > Normal(2) > Low(1) > Unspecified(0)
    /// 同优先级按链接质量排序（RTT越低越好）
    pub fn sort_devices_by_priority(sessions: &mut Vec<ConnectionRecord>) {
        sessions.sort_by(|a, b| {
            // 首先按优先级降序
            let priority_cmp = b.device_priority.cmp(&a.device_priority);
            if priority_cmp != std::cmp::Ordering::Equal {
                return priority_cmp;
            }

            // 相同优先级，按链接质量（RTT）升序
            match (&a.connection_quality, &b.connection_quality) {
                (Some(qa), Some(qb)) => qa.rtt_ms.cmp(&qb.rtt_ms),
                (Some(_), None) => std::cmp::Ordering::Less, // 有质量数据的优先
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });
    }

    /// 选择最优设备（用于单设备推送）
    ///
    /// 选择策略：
    /// 1. 优先选择 Critical 或 High 优先级
    /// 2. 同优先级选择链接质量最好的（RTT 最低）
    pub async fn select_best_device(&self, user_id: &str) -> Result<Option<ConnectionRecord>> {
        let uid = UserId::new(user_id.to_string()).map_err(|e| anyhow::anyhow!(e))?;
        let mut sessions: Vec<ConnectionRecord> = self
            .repository
            .get_user_connections(&uid)
            .await?
            .into_iter()
            .map(|s| ConnectionRecord {
                conversation_id: s.id().as_str().to_string(),
                user_id: user_id.to_string(),
                device_id: s.device_id().as_str().to_string(),
                device_platform: s.device_platform().to_string(),
                server_id: s.server_id().to_string(),
                gateway_id: s.gateway_id().to_string(),
                last_seen: s.last_heartbeat_at(),
                device_priority: s.device_priority().as_i32(),
                token_version: s.token_version().value(),
                connection_quality: s.connection_quality().map(|q| ConnectionQualityRecord {
                    rtt_ms: q.rtt_ms(),
                    packet_loss_rate: q.packet_loss_rate(),
                    last_measure_ts: (q.last_measure_ts().timestamp() * 1000)
                        + (q.last_measure_ts().timestamp_subsec_millis() as i64),
                    network_type: q.network_type().as_str().to_string(),
                    signal_strength: q.signal_strength().unwrap_or_default(),
                }),
            })
            .collect();

        if sessions.is_empty() {
            return Ok(None);
        }

        Self::sort_devices_by_priority(&mut sessions);
        Ok(sessions.into_iter().next())
    }

    /// 获取需要推送的设备列表（根据优先级过滤）
    ///
    /// 过滤策略：
    /// - Critical(4)：始终推送
    /// - High(3)：活跃设备，始终推送
    /// - Normal(2)：普通设备，始终推送
    /// - Low(1)：后台设备，仅在没有更高优先级设备时推送
    /// - Unspecified(0)：视为 Normal
    pub async fn get_push_target_devices(
        &self,
        user_id: &str,
        include_low_priority: bool,
    ) -> Result<Vec<ConnectionRecord>> {
        let uid = UserId::new(user_id.to_string()).map_err(|e| anyhow::anyhow!(e))?;
        let sessions = self.repository.get_user_connections(&uid).await?;

        let result: Vec<ConnectionRecord> = sessions
            .into_iter()
            .map(|s| ConnectionRecord {
                conversation_id: s.id().as_str().to_string(),
                user_id: user_id.to_string(),
                device_id: s.device_id().as_str().to_string(),
                device_platform: s.device_platform().to_string(),
                server_id: s.server_id().to_string(),
                gateway_id: s.gateway_id().to_string(),
                last_seen: s.last_heartbeat_at(),
                device_priority: s.device_priority().as_i32(),
                token_version: s.token_version().value(),
                connection_quality: s.connection_quality().map(|q| ConnectionQualityRecord {
                    rtt_ms: q.rtt_ms(),
                    packet_loss_rate: q.packet_loss_rate(),
                    last_measure_ts: (q.last_measure_ts().timestamp() * 1000)
                        + (q.last_measure_ts().timestamp_subsec_millis() as i64),
                    network_type: q.network_type().as_str().to_string(),
                    signal_strength: q.signal_strength().unwrap_or_default(),
                }),
            })
            .filter(|s| {
                if s.device_priority >= 2 {
                    return true;
                }
                if s.device_priority == 1 {
                    return include_low_priority;
                }
                true
            })
            .collect();

        if result.is_empty() && include_low_priority {
            let sessions_all = self.repository.get_user_connections(&uid).await?;
            let records_all: Vec<ConnectionRecord> = sessions_all
                .into_iter()
                .map(|s| ConnectionRecord {
                    conversation_id: s.id().as_str().to_string(),
                    user_id: user_id.to_string(),
                    device_id: s.device_id().as_str().to_string(),
                    device_platform: s.device_platform().to_string(),
                    server_id: s.server_id().to_string(),
                    gateway_id: s.gateway_id().to_string(),
                    last_seen: s.last_heartbeat_at(),
                    device_priority: s.device_priority().as_i32(),
                    token_version: s.token_version().value(),
                    connection_quality: s.connection_quality().map(|q| ConnectionQualityRecord {
                        rtt_ms: q.rtt_ms(),
                        packet_loss_rate: q.packet_loss_rate(),
                        last_measure_ts: (q.last_measure_ts().timestamp() * 1000)
                            + (q.last_measure_ts().timestamp_subsec_millis() as i64),
                        network_type: q.network_type().as_str().to_string(),
                        signal_strength: q.signal_strength().unwrap_or_default(),
                    }),
                })
                .collect();
            return Ok(records_all);
        }

        Ok(result)
    }

    /// 强制踢出指定设备
    pub async fn kick_device(&self, user_id: &str, conversation_id: &str, reason: &str) -> Result<()> {
        let uid = UserId::new(user_id.to_string()).map_err(|e| anyhow::anyhow!(e))?;
        let sid = ConnectionId::from_string(conversation_id.to_string()).map_err(|e| anyhow::anyhow!(e))?;
        warn!(
            user_id = %user_id,
            conversation_id = %conversation_id,
            reason = %reason,
            "Kicking device"
        );
        self.repository.remove_connection(&sid, &uid).await?;
        Ok(())
    }

    /// 批量踢出设备（用于 Token 版本升级）
    pub async fn batch_kick_devices(
        &self,
        user_id: &str,
        conversation_ids: &[String],
        reason: &str,
    ) -> Result<()> {
        for conversation_id in conversation_ids {
            self.kick_device(user_id, conversation_id, reason).await?;
        }
        Ok(())
    }
}
