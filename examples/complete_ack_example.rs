//! 完整的ACK处理示例
//! 展示ACK模块的所有功能集成使用

use flare_im_ack::ack::metrics::{AckMetrics, PerformanceConfig};
use flare_im_ack::ack::{
    AckModule, AckModuleStats, AckServiceConfig, AckStatus, AckStatusInfo, AckType, ImportanceLevel,
};
use prometheus::Registry;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Flare IM ACK处理完整示例 ===\n");

    // 初始化数据库连接池 (已移除，ACK模块不再直接依赖 PgPool)
    // let db_pool = PgPoolOptions::new() ...

    // 创建Prometheus注册表
    let registry = Registry::new();

    // 初始化监控指标
    let metrics = Arc::new(AckMetrics::new(&registry)?);

    // 配置ACK服务
    let ack_config = AckServiceConfig {
        redis_url: "redis://127.0.0.1:6379/".to_string(),
        redis_ttl: 3600,
        cache_capacity: 10000,
        batch_interval_ms: 100,
        batch_size: 100,
        ..Default::default()
    };

    // 创建ACK模块
    let ack_module = AckModule::new(ack_config).await?;
    println!("✓ ACK模块初始化成功");

    // 模拟处理不同重要性的ACK
    let ack_infos = vec![
        create_test_ack("high_priority_msg_1", "user_1", ImportanceLevel::High),
        create_test_ack("medium_priority_msg_1", "user_2", ImportanceLevel::Medium),
        create_test_ack("low_priority_msg_1", "user_3", ImportanceLevel::Low),
    ];

    // 处理ACK并根据需要归档
    for (i, ack_info) in ack_infos.iter().enumerate() {
        // Note: record_and_archive_ack doesn't exist in AckModule interface we saw?
        // Let's check AckModule again. It has record_ack_status.
        // The error output for "method not found" wasn't explicitly checking record_and_archive_ack but likely it will fail too if not present.
        // src/ack/mod.rs shows record_ack_status. It does NOT show record_and_archive_ack.
        // So I must change this to record_ack_status.
        // wait, step 58 output lines 60-66 shows record_ack_status.
        // It does NOT show record_and_archive_ack.
        // So I must fix this method call too.
        ack_module.record_ack_status(ack_info.clone()).await?;
        println!(
            "✓ 处理ACK #{}: 消息={}, 用户={}, 重要性={:?}",
            i + 1,
            ack_info.message_id,
            ack_info.user_id,
            ack_info.importance
        );
    }

    // 等待批处理完成
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // 查询ACK状态
    println!("\n--- 查询ACK状态 ---");
    for ack_info in &ack_infos {
        let status = ack_module
            .get_ack_status(&ack_info.message_id, &ack_info.user_id)
            .await?;
        if let Some(status) = status {
            println!(
                "消息 '{}' 的ACK状态: {:?}",
                ack_info.message_id, status.status
            );
        } else {
            println!("未找到消息 '{}' 的ACK状态", ack_info.message_id);
        }
    }

    // 检查ACK是否存在
    println!("\n--- 检查ACK存在性 ---");
    for ack_info in &ack_infos {
        let exists = ack_module
            .exists_ack(&ack_info.message_id, &ack_info.user_id)
            .await?;
        println!("消息 '{}' 的ACK是否存在: {}", ack_info.message_id, exists);
    }

    // 获取模块统计信息
    println!("\n--- 模块统计信息 ---");
    let stats = ack_module.get_stats().await?;
    print_stats(&stats);

    // 获取Prometheus指标
    println!("\n--- Prometheus指标 ---");
    let metrics_data = metrics.get_metrics_data(&registry)?;
    println!("{}", metrics_data);

    // 性能优化示例
    println!("\n--- 性能优化示例 ---");
    let performance_config = PerformanceConfig::default();
    println!("性能配置: {:?}", performance_config);

    // 模拟大量ACK处理
    println!("\n--- 批量处理性能测试 ---");
    let start_time = SystemTime::now();
    for i in 0..1000 {
        let importance = if i % 10 == 0 {
            ImportanceLevel::High
        } else if i % 3 == 0 {
            ImportanceLevel::Medium
        } else {
            ImportanceLevel::Low
        };

        // Generate valid ack info
        let ack_info = create_test_ack(
            &format!("bulk_msg_{}", i),
            &format!("bulk_user_{}", i % 100),
            importance,
        );

        ack_module.record_ack_status(ack_info).await?;
    }

    let duration = SystemTime::now().duration_since(start_time)?.as_millis();
    println!("处理1000个ACK耗时: {} ms", duration);

    // 再次获取统计信息
    println!("\n--- 更新后的统计信息 ---");
    let stats = ack_module.get_stats().await?;
    print_stats(&stats);

    println!("\n=== 示例完成 ===");
    Ok(())
}

/// Helper to create AckStatusInfo
fn create_test_ack(msg_id: &str, user_id: &str, importance: ImportanceLevel) -> AckStatusInfo {
    AckStatusInfo {
        message_id: msg_id.to_string(),
        user_id: user_id.to_string(),
        ack_type: Some(AckType::TransportAck),
        status: AckStatus::Received,
        timestamp: get_current_timestamp(),
        importance,
    }
}

/// 获取当前时间戳
fn get_current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// 打印统计信息
fn print_stats(stats: &AckModuleStats) {
    println!("服务统计:");
    println!("  - 缓存大小: {}", stats.service_stats.cache_size);
    println!(
        "  - 批处理队列大小: {}",
        stats.service_stats.batch_queue_size
    );
    println!(
        "  - Redis已使用内存: {} bytes",
        stats.service_stats.redis_stats.used_memory
    );

    // Archive stats removed as they are not available in AckModuleStats
}
