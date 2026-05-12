//! 路由聚合根
//!
//! Route 是路由领域的聚合根，负责维护路由的业务不变式

use flare_server_core::flare_err;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{ErrorCode, Result};

/// 路由聚合根
///
/// 聚合根职责：
/// - 维护路由的业务不变式（SVID 唯一性、端点有效性）
/// - 提供业务方法（注册、更新、删除）
/// - 发布领域事件
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Route {
    /// 服务 ID（SVID），聚合根标识符
    svid: Svid,
    /// 端点地址
    endpoint: Endpoint,
    /// 路由元数据（分片、机房、权重等）
    metadata: RouteMetadata,
    /// 版本号（用于乐观锁）
    version: u64,
}

impl Route {
    /// 创建新的路由聚合根
    ///
    /// # 业务规则
    /// - SVID 不能为空
    /// - 端点地址必须有效（格式验证）
    pub fn new(svid: Svid, endpoint: Endpoint) -> Self {
        Self {
            svid,
            endpoint,
            metadata: RouteMetadata::default(),
            version: 1,
        }
    }

    /// 使用元数据创建路由
    pub fn with_metadata(svid: Svid, endpoint: Endpoint, metadata: RouteMetadata) -> Self {
        Self {
            svid,
            endpoint,
            metadata,
            version: 1,
        }
    }

    /// 更新端点地址
    ///
    /// # 业务规则
    /// - 新端点必须有效
    /// - 版本号递增
    pub fn update_endpoint(&mut self, new_endpoint: Endpoint) -> Result<()> {
        self.endpoint = new_endpoint;
        self.version += 1;
        Ok(())
    }

    /// 更新元数据
    pub fn update_metadata(&mut self, metadata: RouteMetadata) {
        self.metadata = metadata;
        self.version += 1;
    }

    /// 获取 SVID
    pub fn svid(&self) -> &Svid {
        &self.svid
    }

    /// 获取端点
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// 获取元数据
    pub fn metadata(&self) -> &RouteMetadata {
        &self.metadata
    }

    /// 获取版本号
    pub fn version(&self) -> u64 {
        self.version
    }
}

/// SVID 值对象（服务标识符）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Svid(String);

impl Svid {
    /// 创建新的 SVID
    ///
    /// # 验证规则
    /// - 不能为空
    /// - 格式：svid.{service} 或直接 service
    pub fn new(value: String) -> Result<Self> {
        if value.is_empty() {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "SVID cannot be empty"
            ));
        }
        Ok(Self(normalize_svid(&value)))
    }

    /// 从字符串创建（不验证，用于内部使用）
    pub fn from_string_unchecked(value: String) -> Self {
        Self(value)
    }

    /// 获取字符串值
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Svid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 端点值对象
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Endpoint(String);

impl Endpoint {
    /// 创建新的端点
    ///
    /// # 验证规则
    /// - 不能为空
    /// - 格式：http://host:port 或 host:port
    pub fn new(value: String) -> Result<Self> {
        if value.is_empty() {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "Endpoint cannot be empty"
            ));
        }
        // 基本格式验证
        if !value.contains(':') {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "Endpoint must contain ':' (host:port format)"
            ));
        }
        Ok(Self(value))
    }

    /// 从字符串创建（不验证，用于内部使用）
    pub fn from_string_unchecked(value: String) -> Self {
        Self(value)
    }

    /// 获取字符串值
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 路由元数据值对象
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RouteMetadata {
    /// 分片 ID（可选）
    shard_id: Option<usize>,
    /// 可用区（可选）
    availability_zone: Option<String>,
    /// 权重（用于负载均衡）
    weight: u32,
    /// 扩展属性
    attributes: HashMap<String, String>,
}

impl RouteMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_shard(mut self, shard_id: usize) -> Self {
        self.shard_id = Some(shard_id);
        self
    }

    pub fn with_az(mut self, az: String) -> Self {
        self.availability_zone = Some(az);
        self
    }

    pub fn with_weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }

    pub fn with_attribute(mut self, key: String, value: String) -> Self {
        self.attributes.insert(key, value);
        self
    }

    pub fn shard_id(&self) -> Option<usize> {
        self.shard_id
    }

    pub fn availability_zone(&self) -> Option<&str> {
        self.availability_zone.as_deref()
    }

    pub fn weight(&self) -> u32 {
        self.weight
    }

    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }
}

/// 规范化 SVID（将旧格式转换为新格式）
fn normalize_svid(svid: &str) -> String {
    match svid.to_uppercase().as_str() {
        "IM" => "svid.im".to_string(),
        "CUSTOMER_SERVICE" | "CS" => "svid.customer".to_string(),
        "AI_BOT" | "AI" => "svid.ai.bot".to_string(),
        _ => {
            if svid.contains('.') {
                svid.to_string()
            } else {
                format!("svid.{}", svid.to_lowercase())
            }
        }
    }
}
