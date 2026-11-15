-- 插件版本的 Web 包附件：平台审核通过后登记到 Core，撤回后不再下发。
-- 每个应用的镜像地址和批准的桥权限随授权投影下发；这里只记版本级的审核结论。
-- 撤回是终态：同一版本的同一个包不能再登记回来。
CREATE TABLE plugin_web_bundles (
 plugin_id varchar(128) NOT NULL,
 plugin_version varchar(64) NOT NULL,
 bundle_id varchar(64) NOT NULL CHECK(bundle_id ~ '^[a-z0-9][a-z0-9-]{0,63}$'),
 sha256 text NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
 size bigint NOT NULL CHECK(size BETWEEN 1 AND 10485760),
 approved_at bigint NOT NULL DEFAULT (extract(epoch FROM clock_timestamp())*1000)::bigint,
 withdrawn_at bigint,
 updated_at bigint NOT NULL DEFAULT (extract(epoch FROM clock_timestamp())*1000)::bigint,
 PRIMARY KEY(plugin_id,plugin_version,bundle_id),
 FOREIGN KEY(plugin_id,plugin_version) REFERENCES plugin_versions(plugin_id,plugin_version),
 CHECK(withdrawn_at IS NULL OR withdrawn_at>=approved_at)
);
