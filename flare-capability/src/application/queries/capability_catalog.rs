//! 能力目录读模型（与 `ListCapabilities` / 静态注册表对齐）。

use crate::domain::capability::CapabilityDescriptor;

pub fn list_registered_capabilities() -> Vec<CapabilityDescriptor> {
    const PLUGIN: &str = "plugin.rtc";
    const VERSION: &str = "1.0.0";
    const SCOPE: &str = "conversation";
    const VISIBILITY: &str = "public";
    const TIMEOUT_MS: u64 = 30_000;

    fn rtc_descriptor(
        capability_id: &'static str,
        description: &'static str,
        message_types: &[&'static str],
    ) -> CapabilityDescriptor {
        CapabilityDescriptor {
            capability_id: capability_id.into(),
            plugin_id: PLUGIN.into(),
            version: VERSION.into(),
            scope: SCOPE.into(),
            visibility: VISIBILITY.into(),
            permissions: vec!["rtc:call".into()],
            message_types: message_types.iter().copied().map(str::to_string).collect(),
            timeout_ms: TIMEOUT_MS,
            description: description.into(),
        }
    }

    let media_cmds = ["rtc/invite", "rtc/accept", "rtc/end"];

    vec![
        rtc_descriptor("rtc.call.audio", "RTC audio call", &media_cmds),
        rtc_descriptor("rtc.call.video", "RTC video call", &media_cmds),
        rtc_descriptor("rtc.call.accept", "Accept RTC call", &[]),
        rtc_descriptor("rtc.call.reject", "Reject RTC call", &[]),
        rtc_descriptor("rtc.call.end", "End RTC call", &[]),
        rtc_descriptor(
            "rtc.call.join_token",
            "SFU join token (flare-strom-sfu GetJoinToken)",
            &["rtc/join"],
        ),
        rtc_descriptor(
            "rtc.sfu.join_room",
            "SFU JoinRoom (flare-strom-sfu gRPC)",
            &[],
        ),
        rtc_descriptor(
            "rtc.sfu.leave_room",
            "SFU LeaveRoom (flare-strom-sfu gRPC)",
            &[],
        ),
        rtc_descriptor(
            "rtc.sfu.handle_sdp_offer",
            "SFU HandleSdpOffer: browser offer → SFU answer",
            &[],
        ),
        rtc_descriptor(
            "rtc.sfu.handle_sdp_answer",
            "SFU HandleSdpAnswer (plugin may return unimplemented)",
            &[],
        ),
        rtc_descriptor(
            "rtc.sfu.add_ice_candidate",
            "SFU AddIceCandidate (Trickle ICE)",
            &[],
        ),
    ]
}
