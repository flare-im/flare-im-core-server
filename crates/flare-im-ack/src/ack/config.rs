//! ACK配置管理
//! 支持根据不同业务场景动态调整ACK重要性级别配置

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ACK重要性级别配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckImportanceConfig {
    /// 高重要性配置
    pub high: ImportanceLevelConfig,
    /// 中等重要性配置
    pub medium: ImportanceLevelConfig,
    /// 低重要性配置
    pub low: ImportanceLevelConfig,
}

/// 重要性级别配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportanceLevelConfig {
    /// Redis过期时间（秒）
    pub redis_ttl: u64,
    /// 是否立即持久化
    pub immediate_persistence: bool,
    /// 超时时间（秒）
    pub timeout_seconds: u64,
    /// 最大重试次数
    pub max_retries: u32,
}

/// 业务场景配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessScenarioConfig {
    /// 场景名称
    pub scenario_name: String,
    /// 消息类型到重要性级别的映射
    pub message_type_mapping: HashMap<String, String>,
    /// 默认重要性级别
    pub default_importance: String,
}

/// ACK服务配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckServiceConfig {
    /// Redis URL
    pub redis_url: String,
    /// Redis默认过期时间（秒）
    pub redis_ttl: u64,
    /// 内存缓存容量
    pub cache_capacity: usize,
    /// 批量处理间隔（毫秒）
    pub batch_interval_ms: u64,
    /// 批量处理大小
    pub batch_size: usize,
    /// 重要性级别配置
    pub importance_config: AckImportanceConfig,
    /// 业务场景配置
    pub business_scenarios: HashMap<String, BusinessScenarioConfig>,
}

impl Default for AckServiceConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1/".to_string(),
            redis_ttl: 3600, // 1小时
            cache_capacity: 10000,
            batch_interval_ms: 100, // 100毫秒
            batch_size: 100,
            importance_config: AckImportanceConfig {
                high: ImportanceLevelConfig {
                    redis_ttl: 7200, // 2小时
                    immediate_persistence: true,
                    timeout_seconds: 30, // 30秒超时
                    max_retries: 3,
                },
                medium: ImportanceLevelConfig {
                    redis_ttl: 3600, // 1小时
                    immediate_persistence: false,
                    timeout_seconds: 60, // 60秒超时
                    max_retries: 2,
                },
                low: ImportanceLevelConfig {
                    redis_ttl: 1800, // 30分钟
                    immediate_persistence: false,
                    timeout_seconds: 120, // 120秒超时
                    max_retries: 1,
                },
            },
            business_scenarios: {
                let mut scenarios = HashMap::new();

                // 即时通讯场景
                let mut im_mapping = HashMap::new();
                im_mapping.insert("text".to_string(), "high".to_string());
                im_mapping.insert("image".to_string(), "high".to_string());
                im_mapping.insert("voice".to_string(), "high".to_string());
                im_mapping.insert("video".to_string(), "high".to_string());
                im_mapping.insert("file".to_string(), "medium".to_string());
                scenarios.insert(
                    "instant_messaging".to_string(),
                    BusinessScenarioConfig {
                        scenario_name: "即时通讯".to_string(),
                        message_type_mapping: im_mapping,
                        default_importance: "medium".to_string(),
                    },
                );

                // 系统通知场景
                let mut notification_mapping = HashMap::new();
                notification_mapping.insert("system_alert".to_string(), "high".to_string());
                notification_mapping.insert("friend_request".to_string(), "high".to_string());
                notification_mapping.insert("group_invite".to_string(), "high".to_string());
                notification_mapping
                    .insert("general_notification".to_string(), "medium".to_string());
                scenarios.insert(
                    "notification".to_string(),
                    BusinessScenarioConfig {
                        scenario_name: "系统通知".to_string(),
                        message_type_mapping: notification_mapping,
                        default_importance: "low".to_string(),
                    },
                );

                // 状态同步场景
                let mut sync_mapping = HashMap::new();
                sync_mapping.insert("presence".to_string(), "low".to_string());
                sync_mapping.insert("typing_indicator".to_string(), "low".to_string());
                sync_mapping.insert("read_receipt".to_string(), "medium".to_string());
                scenarios.insert(
                    "status_sync".to_string(),
                    BusinessScenarioConfig {
                        scenario_name: "状态同步".to_string(),
                        message_type_mapping: sync_mapping,
                        default_importance: "low".to_string(),
                    },
                );

                scenarios
            },
        }
    }
}

impl AckServiceConfig {
    /// 根据业务场景和消息类型获取重要性级别配置
    pub fn get_importance_config_for_message(
        &self,
        scenario: &str,
        message_type: &str,
    ) -> Option<&ImportanceLevelConfig> {
        // 查找业务场景配置
        if let Some(scenario_config) = self.business_scenarios.get(scenario) {
            // 根据消息类型查找重要性级别
            let importance_level = scenario_config
                .message_type_mapping
                .get(message_type)
                .unwrap_or(&scenario_config.default_importance);

            // 返回对应的配置
            match importance_level.as_str() {
                "high" => Some(&self.importance_config.high),
                "medium" => Some(&self.importance_config.medium),
                "low" => Some(&self.importance_config.low),
                _ => None,
            }
        } else {
            None
        }
    }

    /// 获取默认重要性级别配置
    pub fn get_default_importance_config(&self, level: &str) -> Option<&ImportanceLevelConfig> {
        match level {
            "high" => Some(&self.importance_config.high),
            "medium" => Some(&self.importance_config.medium),
            "low" => Some(&self.importance_config.low),
            _ => None,
        }
    }
}
