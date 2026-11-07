//! 租户配置通知的领域规则：谁能在哪个 topic 命名空间里发布。Core 不解释 topic，只按发布者
//! 登记的前缀放行；`flare.` 留给 Core 自己（插件目录通知不经过这里）。

use flare_im_service_kit::tenant_notice::{ALL_TENANTS, CORE_TOPIC_PREFIX, valid_topic};

/// 一次发布的结果：版本前进并已发布，或已有不低于它的版本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoticeOutcome {
    pub accepted: bool,
    pub current_version: u64,
}

/// 发布被拒绝的原因。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NoticeDenied {
    #[error("invalid topic")]
    InvalidTopic,
    #[error("version must be above zero")]
    InvalidVersion,
    #[error("service may not publish this topic")]
    TopicNotPermitted,
    #[error("service may not publish to every tenant")]
    TenantWideNotPermitted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Publisher {
    service: String,
    prefixes: Vec<String>,
    tenant_wide: bool,
}

/// 登记的发布者：`FLARE_TENANT_NOTICE_PUBLISHERS="flare-social-bridge=social.,*;other=foo."`。
/// 每项 `服务=前缀,前缀`；列表里的 `*` 表示还可以发给全部租户。没有登记的服务一律拒绝。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NoticePublishers {
    publishers: Vec<Publisher>,
}

impl NoticePublishers {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut publishers = Vec::new();
        for entry in raw
            .split(';')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
        {
            let (service, list) = entry
                .split_once('=')
                .ok_or_else(|| format!("{entry:?}: expected service=prefix,..."))?;
            let service = service.trim();
            if service.is_empty() {
                return Err(format!("{entry:?}: empty service"));
            }
            let mut prefixes = Vec::new();
            let mut tenant_wide = false;
            for item in list
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
            {
                if item == "*" {
                    tenant_wide = true;
                    continue;
                }
                let topic_root = item.trim_end_matches('.');
                if !item.ends_with('.')
                    || item.starts_with(CORE_TOPIC_PREFIX)
                    || !valid_topic(&format!("{topic_root}.x"))
                {
                    return Err(format!(
                        "{item:?}: expected a non-core prefix ending in '.'"
                    ));
                }
                prefixes.push(item.to_string());
            }
            if prefixes.is_empty() {
                return Err(format!("{entry:?}: no topic prefix"));
            }
            publishers.push(Publisher {
                service: service.to_string(),
                prefixes,
                tenant_wide,
            });
        }
        Ok(Self { publishers })
    }

    pub fn is_empty(&self) -> bool {
        self.publishers.is_empty()
    }

    /// `service` 能否为 `tenant_id`（`*` 为全部租户）发布 `topic`。
    pub fn authorize(
        &self,
        service: &str,
        tenant_id: &str,
        topic: &str,
    ) -> Result<(), NoticeDenied> {
        if !valid_topic(topic) || topic.starts_with(CORE_TOPIC_PREFIX) {
            return Err(NoticeDenied::InvalidTopic);
        }
        let publisher = self
            .publishers
            .iter()
            .find(|publisher| publisher.service == service)
            .ok_or(NoticeDenied::TopicNotPermitted)?;
        if !publisher
            .prefixes
            .iter()
            .any(|prefix| topic.starts_with(prefix.as_str()))
        {
            return Err(NoticeDenied::TopicNotPermitted);
        }
        if tenant_id == ALL_TENANTS && !publisher.tenant_wide {
            return Err(NoticeDenied::TenantWideNotPermitted);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publishers_parse_and_authorize_their_own_namespace() {
        let publishers =
            NoticePublishers::parse("flare-social-bridge=social.,*; reports=report.").unwrap();
        assert!(
            publishers
                .authorize("flare-social-bridge", "app-a", "social.features")
                .is_ok()
        );
        assert!(
            publishers
                .authorize("flare-social-bridge", "*", "social.features")
                .is_ok()
        );
        assert_eq!(
            publishers.authorize("reports", "*", "report.daily"),
            Err(NoticeDenied::TenantWideNotPermitted)
        );
        assert_eq!(
            publishers.authorize("reports", "app-a", "social.features"),
            Err(NoticeDenied::TopicNotPermitted)
        );
        assert_eq!(
            publishers.authorize("someone", "app-a", "social.features"),
            Err(NoticeDenied::TopicNotPermitted)
        );
        for topic in ["flare.plugin.catalog", "Social.features", "social"] {
            assert_eq!(
                publishers.authorize("flare-social-bridge", "app-a", topic),
                Err(NoticeDenied::InvalidTopic),
                "{topic}"
            );
        }
    }

    #[test]
    fn rejects_malformed_or_core_registrations() {
        for raw in [
            "bridge",
            "=social.",
            "bridge=",
            "bridge=social",
            "bridge=flare.",
            "bridge=*",
        ] {
            assert!(NoticePublishers::parse(raw).is_err(), "{raw:?}");
        }
        assert!(NoticePublishers::parse("").unwrap().is_empty());
    }
}
