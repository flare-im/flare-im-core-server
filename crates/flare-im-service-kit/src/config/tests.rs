//! config 模块测试（外联自 mod.rs，仓库测试外联惯例）。

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use toml::Value;

    use super::*;

    #[test]
    fn registry_mode_defaults_to_existing_config_and_rejects_unknown_values() {
        let mut config = default_config();
        apply_registry_mode_override(&mut config, None).unwrap();
        assert!(config.core.registry.is_some());
        assert!(!config.uses_static_registry());

        apply_registry_mode_override(&mut config, Some("configured")).unwrap();
        assert!(config.core.registry.is_some());
        for invalid in ["", "dns", "consul", "STATIC", "disabled"] {
            assert!(
                apply_registry_mode_override(&mut config, Some(invalid)).is_err(),
                "unknown mode {invalid:?} must fail before service startup"
            );
        }
    }

    #[tokio::test]
    async fn static_registry_mode_skips_registration_discovery_and_service_override() {
        let mut config = default_config();
        let service_registry = config.core.registry.clone().unwrap();
        apply_registry_mode_override(&mut config, Some("static")).unwrap();
        assert!(config.core.registry.is_none());
        assert!(config.uses_static_registry());

        let runtime = ServiceRuntimeConfig {
            registry: Some(service_registry),
            ..Default::default()
        };
        assert!(
            config
                .compose_service_config(&runtime, "test-service")
                .registry
                .is_none(),
            "service-specific registry must not override the process static mode"
        );

        let address = "127.0.0.1:50061".parse().unwrap();
        assert!(
            crate::discovery::init_from_config(&config, "test-service", address, None)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            crate::discovery::register_service_from_config(&config, "test-service", address, None,)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            crate::discovery::create_discover_from_config(&config, "test-service")
                .await
                .unwrap()
                .is_none()
        );
    }

    fn jetstream_test_profile(options: HashMap<String, String>) -> JetStreamClusterConfig {
        JetStreamClusterConfig {
            url: "nats://localhost:4222".to_string(),
            stream_name: Some("FLARE_PUSH".to_string()),
            subjects: vec!["flare.im.push.*".to_string()],
            options,
            ..Default::default()
        }
    }

    #[test]
    fn real_base_push_stream_retains_configured_one_day_limit() {
        let base_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/base.toml");
        let raw = fs::read_to_string(&base_path).expect("read real base config");
        let mut value: Value = toml::from_str(&raw).expect("parse real base config");
        expand_env_placeholders_in_value_with(&mut value, &base_path, |_| None)
            .expect("resolve defaults without process environment");
        let config: FlareAppConfig = value.try_into().expect("deserialize real base config");
        let push = config
            .jetstream_topology()
            .into_iter()
            .find(|spec| spec.stream_name == "FLARE_PUSH")
            .expect("push stream topology");
        assert_eq!(push.max_age, Duration::from_secs(86_400));
        assert_eq!(push.duplicate_window, Duration::from_secs(600));
        assert_eq!(push.max_bytes, 1_073_741_824);
        assert_eq!(push.num_replicas, 1);
        let dlq = config
            .jetstream_topology()
            .into_iter()
            .find(|spec| spec.stream_name == "FLARE_DLQ")
            .expect("framework dead-letter stream topology");
        assert_eq!(dlq.subjects, vec!["flare.im.dlq.>"]);
        assert_eq!(dlq.max_age, Duration::from_secs(7 * 86_400));
    }

    #[test]
    fn jetstream_stream_limits_and_replicas_reach_runtime_spec() {
        let mut config = default_config();
        config.jetstream.insert(
            "push".to_string(),
            jetstream_test_profile(HashMap::from([
                ("max_age_seconds".to_string(), "172800".to_string()),
                ("duplicate_window_seconds".to_string(), "120".to_string()),
                ("max_bytes".to_string(), "2147483648".to_string()),
                ("num_replicas".to_string(), "3".to_string()),
            ])),
        );
        let spec = config.jetstream_topology().remove(0);
        assert_eq!(spec.max_age, Duration::from_secs(172_800));
        assert_eq!(spec.duplicate_window, Duration::from_secs(120));
        assert_eq!(spec.max_bytes, 2_147_483_648);
        assert_eq!(spec.num_replicas, 3);
    }

    #[test]
    fn invalid_jetstream_options_fail_validation_instead_of_using_defaults() {
        for (key, value) in [
            ("max_age_seconds", "0"),
            ("duplicate_window_seconds", "nope"),
            ("max_bytes", "9223372036854775808"),
            ("num_replicas", "6"),
            ("retention", "limits"),
        ] {
            let mut config = default_config();
            config.jetstream.insert(
                "push".to_string(),
                jetstream_test_profile(HashMap::from([(key.to_string(), value.to_string())])),
            );
            let error = config
                .validate_references()
                .expect_err("invalid stream option");
            assert!(error.to_string().contains(key), "{key}: {error}");
        }
    }

    #[test]
    fn incomplete_or_duplicate_jetstream_stream_profiles_fail_validation() {
        let mut config = default_config();
        let mut incomplete = jetstream_test_profile(HashMap::new());
        incomplete.subjects.clear();
        config.jetstream.insert("push".to_string(), incomplete);
        assert!(config.validate_references().is_err());

        config
            .jetstream
            .insert("push".to_string(), jetstream_test_profile(HashMap::new()));
        config.jetstream.insert(
            "duplicate".to_string(),
            jetstream_test_profile(HashMap::new()),
        );
        assert!(config.validate_references().is_err());
    }

    #[test]
    fn env_placeholders_expand_in_nested_toml_values() {
        let mut value: Value = toml::from_str(
            r#"
            [object_storage.default]
            access_key = "${FLARE_TEST_ACCESS_KEY}"
            endpoint = "https://${FLARE_TEST_BUCKET}.example.com"
            subjects = ["flare.${FLARE_TEST_BUCKET}.*"]
            "#,
        )
        .expect("valid toml");

        expand_env_placeholders_in_value_with(&mut value, Path::new("config/test.toml"), |name| {
            match name {
                "FLARE_TEST_ACCESS_KEY" => Some("ak-test".to_string()),
                "FLARE_TEST_BUCKET" => Some("media".to_string()),
                _ => None,
            }
        })
        .expect("placeholders expand");

        let storage = value
            .get("object_storage")
            .and_then(|v| v.get("default"))
            .expect("object storage profile");
        assert_eq!(
            storage.get("access_key").and_then(Value::as_str),
            Some("ak-test")
        );
        assert_eq!(
            storage.get("endpoint").and_then(Value::as_str),
            Some("https://media.example.com")
        );
        assert_eq!(
            storage
                .get("subjects")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(Value::as_str),
            Some("flare.media.*")
        );
    }

    #[test]
    fn env_placeholders_fail_when_variable_is_missing() {
        let mut value = Value::String("${FLARE_TEST_MISSING_SECRET}".to_string());
        let err = expand_env_placeholders_in_value_with(
            &mut value,
            Path::new("config/test.toml"),
            |_| None,
        )
        .expect_err("missing env var must fail");

        assert!(
            err.to_string().contains("FLARE_TEST_MISSING_SECRET"),
            "error should name the missing variable: {err}"
        );
    }

    #[test]
    fn env_placeholders_fall_back_to_inline_default() {
        let mut value: Value = toml::from_str(
            r#"
            [postgres.media]
            url = "postgres://${FLARE_TEST_PG_USER:-flare}@${FLARE_TEST_PG_HOST:-localhost}:25432/flare2"
            "#,
        )
        .expect("valid toml");

        // 一个设了、一个没设：设的走环境变量，没设的走内联默认值。
        expand_env_placeholders_in_value_with(&mut value, Path::new("config/test.toml"), |name| {
            match name {
                "FLARE_TEST_PG_HOST" => Some("postgres".to_string()),
                _ => None,
            }
        })
        .expect("defaults make missing variables non-fatal");

        assert_eq!(
            value
                .get("postgres")
                .and_then(|v| v.get("media"))
                .and_then(|v| v.get("url"))
                .and_then(Value::as_str),
            Some("postgres://flare@postgres:25432/flare2")
        );
    }

    #[test]
    fn env_placeholder_empty_value_uses_default() {
        // compose 里声明但没赋值的变量会展开成空串。若把空串当「已设置」，
        // 拼出来的会是 `postgres://:@host` 这种连不上的串，所以空串必须走默认值。
        let mut value = Value::String("${FLARE_TEST_EMPTY:-fallback}".to_string());
        expand_env_placeholders_in_value_with(&mut value, Path::new("config/test.toml"), |_| {
            Some(String::new())
        })
        .expect("empty value falls back");
        assert_eq!(value.as_str(), Some("fallback"));
    }

    #[test]
    fn env_placeholder_without_default_still_required() {
        // 加了默认值语法之后，无默认值的写法必须**保持原样报错**——
        // 否则密钥类占位符会静默变成空串，正是这道校验要防的事。
        let mut value = Value::String("${FLARE_TEST_STILL_REQUIRED}".to_string());
        let err = expand_env_placeholders_in_value_with(
            &mut value,
            Path::new("config/test.toml"),
            |_| None,
        )
        .expect_err("no default means still required");
        assert!(
            err.to_string().contains("FLARE_TEST_STILL_REQUIRED"),
            "error should name the missing variable: {err}"
        );
    }

    #[test]
    fn env_placeholder_default_may_be_empty() {
        let mut value = Value::String("redis://${FLARE_TEST_AUTH:-}host".to_string());
        expand_env_placeholders_in_value_with(&mut value, Path::new("config/test.toml"), |_| None)
            .expect("empty default is legal");
        assert_eq!(value.as_str(), Some("redis://host"));
    }

    /// 拿**真的** `config/base.toml` 过一遍真的展开逻辑。
    ///
    /// 上面那几条测的是展开函数本身；这条测的是「仓库里那份配置文件到底会解析成什么」。
    /// 两者缺一不可——占位符语法写对了、但 base.toml 里少个 `:-` 或者默认值被人顺手
    /// 改成容器服务名，单看函数级测试是发现不了的。
    ///
    /// 不碰进程环境变量：用自定义 lookup 模拟「没设」与「容器里设了」两种情形，
    /// 免得与并行跑的其它测试互相干扰。
    #[test]
    fn real_base_config_resolves_for_both_local_and_container() {
        let base_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config/base.toml")
            .canonicalize()
            .expect("locate config/base.toml");
        let raw = std::fs::read_to_string(&base_path).expect("read base.toml");

        // ── 情形一：什么都没设（本机开发）→ 必须还是原来那套地址 ──
        let mut local: Value = toml::from_str(&raw).expect("valid toml");
        expand_env_placeholders_in_value_with(&mut local, &base_path, |_| None)
            .expect("本机开发不设任何变量也必须能解析");

        let pg = |cfg: &Value| -> String {
            cfg.get("postgres")
                .and_then(|v| v.get("media"))
                .and_then(|v| v.get("url"))
                .and_then(Value::as_str)
                .expect("postgres.media.url")
                .to_string()
        };
        assert_eq!(
            pg(&local),
            "postgres://flare:flare123@localhost:25432/flare2",
            "本机默认值被改动了——依赖 dev 栈的开发流程和测试会集体连不上"
        );

        // ── 情形二：容器里注入服务名 → 必须解析成容器地址 ──
        let mut container: Value = toml::from_str(&raw).expect("valid toml");
        expand_env_placeholders_in_value_with(&mut container, &base_path, |name| match name {
            "FLARE_POSTGRES_URL" => Some("postgres://u:p@postgres:5432/flare2".to_string()),
            "FLARE_REDIS_URL_BASE" => Some("redis://redis:6379".to_string()),
            _ => None,
        })
        .expect("容器里只覆盖部分变量，其余仍走默认值");

        assert_eq!(pg(&container), "postgres://u:p@postgres:5432/flare2");

        // 覆盖 redis 基址后，6 个不同 db 序号必须都跟着变（它们共用一个变量）。
        let redis_urls: Vec<&str> = container
            .get("redis")
            .and_then(Value::as_table)
            .expect("redis table")
            .values()
            .filter_map(|v| v.get("url").and_then(Value::as_str))
            .collect();
        assert!(
            !redis_urls.is_empty()
                && redis_urls
                    .iter()
                    .all(|u| u.starts_with("redis://redis:6379/")),
            "redis 基址覆盖后应全部指向容器，实际：{redis_urls:?}"
        );
    }

    /// 不传路径时也必须认 `FLARE_CONFIG_PATH`。
    ///
    /// 这条守的是一次真实故障：配置有两个入口，
    ///   `load_app_config_from_env()` 读 FLARE_CONFIG_PATH（多数服务走它），
    ///   `load_config(None)` 只找 CWD 下的 config/（注册路径走它），
    /// 谁先跑谁定下 APP_CONFIG（OnceLock）。容器里 CWD 没有 config/，于是先跑注册的
    /// 服务全部候选落空、**静默回退到默认配置**（只有一条 warn），注册中心地址变成
    /// 默认的 localhost:28500 —— api-gateway 与 admin-gateway 注册失败反复重启，
    /// 而同一份 base.toml 下其余 13 个服务一切正常，极易误判成配置文件或网络问题。
    #[test]
    fn config_candidates_honour_env_path_when_unspecified() {
        let with_env = config_candidates(None, Some("/etc/flare/config"));
        assert_eq!(
            with_env.first().map(|p| p.to_string_lossy().to_string()),
            Some("/etc/flare/config".to_string()),
            "环境变量给的路径必须排在最前，否则容器里会先命中不存在的相对路径"
        );
        assert!(
            with_env.len() > 1,
            "环境变量路径之后仍要保留相对路径兜底，本机开发不受影响"
        );

        // 显式传路径时**不能**被环境变量顶掉——调用方明确指定了就以它为准。
        let explicit = config_candidates(Some("/custom/path"), Some("/etc/flare/config"));
        assert_eq!(explicit, vec![PathBuf::from("/custom/path")]);
    }

    /// 没有环境变量时，候选列表必须与历史行为逐项一致。
    ///
    /// 参数化的前提是不改变本机行为；这条把「本机开发那条路」钉死。
    #[test]
    fn config_candidates_unchanged_without_env() {
        assert_eq!(
            config_candidates(None, None),
            vec![
                PathBuf::from("config"),
                PathBuf::from("config.toml"),
                PathBuf::from("flare-core-server/config"),
            ]
        );
    }

    #[test]
    fn mq_default_backend_accepts_only_canonical_values() {
        let mut config = default_config();
        config.mq.default_backend = "nats".to_string();
        config
            .validate_references()
            .expect("nats is the canonical JetStream backend selector");

        config.mq.default_backend = "jetstream".to_string();
        let err = config
            .validate_references()
            .expect_err("jetstream is a profile family, not a backend selector");
        assert!(
            err.to_string().contains("expected 'nats' or 'kafka'"),
            "error should describe canonical MQ backend values: {err}"
        );
    }
