//! ACK告警管理
//! 实现ACK链路的告警规则和通知机制

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn, error};

/// 告警规则
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    /// 规则ID
    pub id: String,
    /// 规则名称
    pub name: String,
    /// 告警条件
    pub condition: AlertCondition,
    /// 告警级别
    pub severity: AlertSeverity,
    /// 是否启用
    pub enabled: bool,
    /// 告警通知配置
    pub notification_config: NotificationConfig,
}

/// 告警条件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertCondition {
    /// 指标名称
    pub metric_name: String,
    /// 操作符
    pub operator: ComparisonOperator,
    /// 阈值
    pub threshold: f64,
    /// 持续时间（秒）
    pub duration: u64,
    /// 评估周期（秒）
    pub evaluation_period: u64,
}

/// 比较操作符
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComparisonOperator {
    GreaterThan,
    LessThan,
    Equal,
    NotEqual,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

/// 告警级别
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertSeverity {
    /// 信息
    Info,
    /// 警告
    Warning,
    /// 错误
    Error,
    /// 严重
    Critical,
}

/// 通知配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationConfig {
    /// 通知渠道
    pub channels: Vec<NotificationChannel>,
    /// 通知模板
    pub template: String,
}

/// 通知渠道
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NotificationChannel {
    /// 邮件
    Email(String),
    /// Slack
    Slack(String),
    /// Webhook
    Webhook(String),
    /// 控制台
    Console,
}

/// 告警事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    /// 规则ID
    pub rule_id: String,
    /// 规则名称
    pub rule_name: String,
    /// 告警级别
    pub severity: AlertSeverity,
    /// 告警消息
    pub message: String,
    /// 触发时间
    pub triggered_at: u64,
    /// 相关指标值
    pub metric_value: f64,
}

/// 告警管理器
pub struct AlertManager {
    /// 告警规则
    rules: Arc<RwLock<HashMap<String, AlertRule>>>,
    /// 告警历史
    alert_history: Arc<RwLock<Vec<AlertEvent>>>,
    /// 最大历史记录数
    max_history_size: usize,
}

impl AlertManager {
    /// 创建新的告警管理器
    pub fn new(max_history_size: usize) -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
            alert_history: Arc::new(RwLock::new(Vec::new())),
            max_history_size,
        }
    }

    /// 添加告警规则
    pub async fn add_rule(&self, rule: AlertRule) {
        let mut rules = self.rules.write().await;
        let rule_name = rule.name.clone();
        rules.insert(rule.id.clone(), rule);
        info!("Added alert rule: {}", rule_name);
    }

    /// 删除告警规则
    pub async fn remove_rule(&self, rule_id: &str) {
        let mut rules = self.rules.write().await;
        if let Some(rule) = rules.remove(rule_id) {
            info!("Removed alert rule: {}", rule.name);
        }
    }

    /// 获取所有告警规则
    pub async fn get_rules(&self) -> HashMap<String, AlertRule> {
        let rules = self.rules.read().await;
        rules.clone()
    }

    /// 评估指标并触发告警
    pub async fn evaluate_metric(&self, metric_name: &str, value: f64) {
        let rules = self.rules.read().await;
        
        for rule in rules.values() {
            if !rule.enabled || rule.condition.metric_name != metric_name {
                continue;
            }
            
            let should_trigger = match rule.condition.operator {
                ComparisonOperator::GreaterThan => value > rule.condition.threshold,
                ComparisonOperator::LessThan => value < rule.condition.threshold,
                ComparisonOperator::Equal => (value - rule.condition.threshold).abs() < f64::EPSILON,
                ComparisonOperator::NotEqual => (value - rule.condition.threshold).abs() >= f64::EPSILON,
                ComparisonOperator::GreaterThanOrEqual => value >= rule.condition.threshold,
                ComparisonOperator::LessThanOrEqual => value <= rule.condition.threshold,
            };
            
            if should_trigger {
                self.trigger_alert(rule, value).await;
            }
        }
    }

    /// 触发告警
    async fn trigger_alert(&self, rule: &AlertRule, metric_value: f64) {
        let alert_event = AlertEvent {
            rule_id: rule.id.clone(),
            rule_name: rule.name.clone(),
            severity: rule.severity.clone(),
            message: self.format_alert_message(&rule.notification_config.template, rule, metric_value),
            triggered_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            metric_value,
        };

        // 记录告警历史
        {
            let mut history = self.alert_history.write().await;
            history.push(alert_event.clone());
            
            // 保持历史记录在限制范围内
            if history.len() > self.max_history_size {
                let drain_count = history.len() - self.max_history_size;
                history.drain(0..drain_count);
            }
        }

        // 发送通知
        self.send_notifications(rule, &alert_event).await;
        
        // 记录告警日志
        match rule.severity {
            AlertSeverity::Info => info!("Alert triggered: {}", alert_event.message),
            AlertSeverity::Warning => warn!("Alert triggered: {}", alert_event.message),
            AlertSeverity::Error => error!("Alert triggered: {}", alert_event.message),
            AlertSeverity::Critical => error!("Critical alert triggered: {}", alert_event.message),
        }
    }

    /// 格式化告警消息
    fn format_alert_message(&self, template: &str, rule: &AlertRule, metric_value: f64) -> String {
        template
            .replace("{rule_name}", &rule.name)
            .replace("{metric_name}", &rule.condition.metric_name)
            .replace("{threshold}", &rule.condition.threshold.to_string())
            .replace("{metric_value}", &metric_value.to_string())
            .replace("{severity}", &format!("{:?}", rule.severity))
    }

    /// 发送通知
    async fn send_notifications(&self, rule: &AlertRule, alert_event: &AlertEvent) {
        for channel in &rule.notification_config.channels {
            match channel {
                NotificationChannel::Email(email) => {
                    self.send_email_notification(email, alert_event).await;
                }
                NotificationChannel::Slack(webhook_url) => {
                    self.send_slack_notification(webhook_url, alert_event).await;
                }
                NotificationChannel::Webhook(url) => {
                    self.send_webhook_notification(url, alert_event).await;
                }
                NotificationChannel::Console => {
                    println!("ALERT: {}", alert_event.message);
                }
            }
        }
    }

    /// 发送邮件通知
    async fn send_email_notification(&self, _email: &str, _alert_event: &AlertEvent) {
        // 实际实现中需要集成邮件发送服务
        info!("Would send email notification to {}", _email);
    }

    /// 发送Slack通知
    async fn send_slack_notification(&self, _webhook_url: &str, _alert_event: &AlertEvent) {
        // 实际实现中需要集成Slack webhook
        info!("Would send Slack notification to {}", _webhook_url);
    }

    /// 发送Webhook通知
    async fn send_webhook_notification(&self, _url: &str, _alert_event: &AlertEvent) {
        // 实际实现中需要发送HTTP请求
        info!("Would send webhook notification to {}", _url);
    }

    /// 获取告警历史
    pub async fn get_alert_history(&self, limit: Option<usize>) -> Vec<AlertEvent> {
        let history = self.alert_history.read().await;
        let limit = limit.unwrap_or(history.len());
        history.iter().take(limit).cloned().collect()
    }

    /// 根据严重程度过滤告警历史
    pub async fn get_alerts_by_severity(&self, severity: AlertSeverity) -> Vec<AlertEvent> {
        let history = self.alert_history.read().await;
        history
            .iter()
            .filter(|event| event.severity == severity)
            .cloned()
            .collect()
    }
}

/// 默认告警规则配置
pub fn default_ack_alert_rules() -> Vec<AlertRule> {
    vec![
        // 高重要性ACK处理延迟过高告警
        AlertRule {
            id: "high_ack_latency".to_string(),
            name: "高重要性ACK处理延迟过高".to_string(),
            condition: AlertCondition {
                metric_name: "ack_processing_latency_by_importance".to_string(),
                operator: ComparisonOperator::GreaterThan,
                threshold: 0.1, // 100ms
                duration: 60,   // 持续1分钟
                evaluation_period: 30, // 每30秒评估一次
            },
            severity: AlertSeverity::Error,
            enabled: true,
            notification_config: NotificationConfig {
                channels: vec![NotificationChannel::Console],
                template: "高重要性ACK处理延迟过高: {metric_value}s > {threshold}s".to_string(),
            },
        },
        // 批处理队列积压告警
        AlertRule {
            id: "batch_queue_backlog".to_string(),
            name: "批处理队列积压".to_string(),
            condition: AlertCondition {
                metric_name: "ack_batch_queue_size".to_string(),
                operator: ComparisonOperator::GreaterThan,
                threshold: 1000.0,
                duration: 120,  // 持续2分钟
                evaluation_period: 60, // 每分钟评估一次
            },
            severity: AlertSeverity::Warning,
            enabled: true,
            notification_config: NotificationConfig {
                channels: vec![NotificationChannel::Console],
                template: "批处理队列积压: {metric_value} > {threshold}".to_string(),
            },
        },
        // 缓存命中率过低告警
        AlertRule {
            id: "low_cache_hit_rate".to_string(),
            name: "缓存命中率过低".to_string(),
            condition: AlertCondition {
                metric_name: "ack_cache_hit_rate".to_string(),
                operator: ComparisonOperator::LessThan,
                threshold: 80.0, // 低于80%
                duration: 300,   // 持续5分钟
                evaluation_period: 60, // 每分钟评估一次
            },
            severity: AlertSeverity::Warning,
            enabled: true,
            notification_config: NotificationConfig {
                channels: vec![NotificationChannel::Console],
                template: "缓存命中率过低: {metric_value}% < {threshold}%".to_string(),
            },
        },
        // Redis连接数过高告警
        AlertRule {
            id: "high_redis_connections".to_string(),
            name: "Redis连接数过高".to_string(),
            condition: AlertCondition {
                metric_name: "ack_redis_connections".to_string(),
                operator: ComparisonOperator::GreaterThan,
                threshold: 100.0,
                duration: 60,   // 持续1分钟
                evaluation_period: 30, // 每30秒评估一次
            },
            severity: AlertSeverity::Error,
            enabled: true,
            notification_config: NotificationConfig {
                channels: vec![NotificationChannel::Console],
                template: "Redis连接数过高: {metric_value} > {threshold}".to_string(),
            },
        },
        // ACK处理错误率过高告警
        AlertRule {
            id: "high_ack_error_rate".to_string(),
            name: "ACK处理错误率过高".to_string(),
            condition: AlertCondition {
                metric_name: "ack_processing_errors_total".to_string(),
                operator: ComparisonOperator::GreaterThan,
                threshold: 10.0, // 每分钟超过10个错误
                duration: 60,    // 持续1分钟
                evaluation_period: 60, // 每分钟评估一次
            },
            severity: AlertSeverity::Critical,
            enabled: true,
            notification_config: NotificationConfig {
                channels: vec![NotificationChannel::Console],
                template: "ACK处理错误率过高: {metric_value} > {threshold}/minute".to_string(),
            },
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_alert_manager() {
        let alert_manager = AlertManager::new(100);
        
        // 添加默认告警规则
        for rule in default_ack_alert_rules() {
            alert_manager.add_rule(rule).await;
        }
        
        // 获取规则
        let rules = alert_manager.get_rules().await;
        assert_eq!(rules.len(), 5);
        
        // 评估指标
        alert_manager.evaluate_metric("ack_processing_latency_by_importance", 0.15).await;
        alert_manager.evaluate_metric("ack_batch_queue_size", 1500.0).await;
        alert_manager.evaluate_metric("ack_cache_hit_rate", 75.0).await;
        
        // 获取告警历史
        let history = alert_manager.get_alert_history(None).await;
        assert!(!history.is_empty());
        
        // 根据严重程度过滤
        let errors = alert_manager.get_alerts_by_severity(AlertSeverity::Error).await;
        assert!(!errors.is_empty());
    }
}