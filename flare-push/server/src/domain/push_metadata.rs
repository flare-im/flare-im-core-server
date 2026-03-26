//! PushOptions.metadata 与请求级 metadata 合并；同 key 时请求级覆盖。

use std::collections::HashMap;

use flare_proto::push::PushOptions;

pub fn merge_envelope_metadata(
    options: &Option<PushOptions>,
    extra: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut m = options
        .as_ref()
        .map(|o| o.metadata.clone())
        .unwrap_or_default();
    for (k, v) in extra {
        m.insert(k.clone(), v.clone());
    }
    m
}
