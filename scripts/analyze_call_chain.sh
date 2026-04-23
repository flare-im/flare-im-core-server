#!/usr/bin/env bash
# 全链路通话日志诊断：
# - orchestrator: CallSignal invite/accept/renegotiate/ice/hangup 时序、transport 完整性
# - capability: rtc.call.* 调度、SFU 房间/成员事件（按时间窗）
#
# 用法:
#   ./scripts/analyze_call_chain.sh <call_id> [logs_dir]
# 示例:
#   ./scripts/analyze_call_chain.sh call-b3b54cdf-1575-45c9-815c-eb9ac4da636c
#   ./scripts/analyze_call_chain.sh call-xxx /abs/path/to/logs

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <call_id> [logs_dir]"
  exit 1
fi

CALL_ID="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOGS_DIR="${2:-$PROJECT_ROOT/logs}"

ORCH_LOG="$LOGS_DIR/flare-message-orchestrator.log"
CAP_LOG="$LOGS_DIR/flare-capability.log"

if [[ ! -f "$ORCH_LOG" ]]; then
  echo "ERROR: orchestrator log not found: $ORCH_LOG"
  exit 2
fi

if [[ ! -f "$CAP_LOG" ]]; then
  echo "WARN: capability log not found: $CAP_LOG (continue with orchestrator only)"
fi

python3 - "$CALL_ID" "$ORCH_LOG" "$CAP_LOG" <<'PY'
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

call_id = sys.argv[1]
orch_path = Path(sys.argv[2])
cap_path = Path(sys.argv[3])
analyzer_hint = orch_path.parent.parent / "scripts" / "analyze_call_chain.sh"

sig_re = re.compile(r'signal: Some\((Invite|Accept|Reject|Hangup|IceCandidate|Renegotiate)')
from_re = re.compile(r'from_user_id: "([^"]*)"')
event_id_re = re.compile(r'event_id: "([^"]*)"')
conv_re = re.compile(r'conversation_id: "([^"]*)"')
candidate_typ_re = re.compile(r'\btyp (host|srflx|relay|prflx)\b')
candidate_ip_re = re.compile(
    r'candidate:[^"]*\s(?:udp|tcp)\s\d+\s([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)\s\d+\s'
)
room_id_re = re.compile(r'room_id: "([^"]+)"')
ws_base_re = re.compile(r'signaling_ws_base: Some\("([^"]+)"\)')
ext_offer_re = re.compile(r'"flare_sdp_type":\s*"offer"|flareSdpType":\s*"offer"')
ext_answer_re = re.compile(r'"flare_sdp_type":\s*"answer"|flareSdpType":\s*"answer"')
ts_re = re.compile(r'^(\d{4}-\d{2}-\d{2}T[0-9:.]+Z)')

def parse_ts(line: str):
    m = ts_re.match(line)
    if not m:
        return None
    raw = m.group(1)
    # 兼容 nanos: fromisoformat 最多 6 位小数
    if "." in raw:
        pfx, rest = raw.split(".", 1)
        frac = rest[:-1]  # remove Z
        frac = (frac + "000000")[:6]
        raw = f"{pfx}.{frac}Z"
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None

raw_lines = 0
timeline = []
events_by_id = {}
transport_none_by_signal = Counter()
transport_non_none_by_signal = Counter()
signal_counts = Counter()
signal_by_user = defaultdict(Counter)
ice_typ_counts = Counter()
ice_typ_by_user = defaultdict(Counter)
suspicious_ips = Counter()
room_ids = Counter()
ws_bases = Counter()
conversations = Counter()
degraded_count = 0
reneg_offer = 0
reneg_answer = 0

first_ts = None
last_ts = None

with orch_path.open("r", errors="ignore") as f:
    for line in f:
        if call_id not in line or "CallSignalEvent" not in line:
            continue
        raw_lines += 1
        ts = parse_ts(line)
        if ts:
            first_ts = ts if first_ts is None else min(first_ts, ts)
            last_ts = ts if last_ts is None else max(last_ts, ts)
        sig_m = sig_re.search(line)
        if not sig_m:
            continue
        signal = sig_m.group(1)
        from_user = from_re.search(line).group(1) if from_re.search(line) else ""
        event_id = event_id_re.search(line).group(1) if event_id_re.search(line) else ""
        conv = conv_re.search(line).group(1) if conv_re.search(line) else ""
        transport_none = "transport: None" in line
        if transport_none:
            transport_none_by_signal[signal] += 1
        else:
            transport_non_none_by_signal[signal] += 1

        if "flare_rtc_enrich" in line and "degraded" in line:
            degraded_count += 1
        if signal == "Renegotiate":
            if ext_offer_re.search(line):
                reneg_offer += 1
            if ext_answer_re.search(line):
                reneg_answer += 1
        if signal == "IceCandidate":
            tm = candidate_typ_re.search(line)
            typ = tm.group(1) if tm else "unknown"
            ice_typ_counts[typ] += 1
            ice_typ_by_user[from_user][typ] += 1
            ipm = candidate_ip_re.search(line)
            if ipm:
                ip = ipm.group(1)
                if ip.startswith("26.26.") or ip.startswith("169.254."):
                    suspicious_ips[ip] += 1

        for m in room_id_re.finditer(line):
            room_ids[m.group(1)] += 1
        for m in ws_base_re.finditer(line):
            ws_bases[m.group(1)] += 1
        if conv:
            conversations[conv] += 1

        # 用 event_id 去重：同一 event 在 trace/debug 链路会重复打印多次
        dedup_key = event_id or f"{signal}|{from_user}|{ts}"
        if dedup_key in events_by_id:
            continue
        events_by_id[dedup_key] = {
            "ts": ts,
            "signal": signal,
            "from_user": from_user,
            "event_id": event_id,
            "conversation_id": conv,
            "transport_none": transport_none,
        }
        signal_counts[signal] += 1
        signal_by_user[from_user][signal] += 1
        timeline.append(events_by_id[dedup_key])

timeline.sort(key=lambda x: (x["ts"] or datetime.min.replace(tzinfo=timezone.utc), x["event_id"]))

# capability（按时间窗 + call_id 过滤）
cap_lines = 0
cap_dispatch = Counter()
cap_room_ids = Counter()
cap_user_ids = Counter()
cap_events = Counter()
cap_first_ts = None
cap_last_ts = None

cap_dispatch_re = re.compile(r'capability.dispatch capability_id=([a-zA-Z0-9_.-]+)')
cap_room_re = re.compile(r'room_id: RoomId\(([^)]+)\)')
cap_user_re = re.compile(r'user_id: UserId\("([^"]+)"\)')
cap_evt_re = re.compile(r'event=(CallStarted|MemberJoined|MemberLeft|VideoPublished|AudioPublished)')

def in_window(ts):
    if first_ts is None or last_ts is None or ts is None:
        return True
    return (first_ts - timedelta(seconds=60)) <= ts <= (last_ts + timedelta(seconds=60))

if cap_path.exists():
    with cap_path.open("r", errors="ignore") as f:
        for line in f:
            ts = parse_ts(line)
            if call_id in line:
                cap_lines += 1
                if ts:
                    cap_first_ts = ts if cap_first_ts is None else min(cap_first_ts, ts)
                    cap_last_ts = ts if cap_last_ts is None else max(cap_last_ts, ts)
                for m in cap_room_re.finditer(line):
                    cap_room_ids[m.group(1)] += 1
                for m in cap_user_re.finditer(line):
                    cap_user_ids[m.group(1)] += 1
                em = cap_evt_re.search(line)
                if em:
                    cap_events[em.group(1)] += 1
                continue
            # 调度日志通常不带 call_id，用时间窗兜底
            if "capability.dispatch capability_id=" in line and in_window(ts):
                dm = cap_dispatch_re.search(line)
                if dm:
                    cap_dispatch[dm.group(1)] += 1

def pct(a, b):
    if b <= 0:
        return "0.0%"
    return f"{(a * 100.0 / b):.1f}%"

print("=" * 78)
print("Flare Call Chain Analyzer")
print("=" * 78)
print(f"call_id: {call_id}")
print(f"orchestrator_log: {orch_path}")
print(f"capability_log:   {cap_path if cap_path.exists() else '(missing)'}")
if first_ts and last_ts:
    print(f"time_window_utc:  {first_ts.isoformat()} -> {last_ts.isoformat()}")
print()

print("[Orchestrator]")
print(f"- raw_call_lines:            {raw_lines}")
print(f"- dedup_call_events:         {len(timeline)}")
print(f"- conversations:             {', '.join(conversations.keys()) if conversations else '(none)'}")
print(f"- sfu_enrich_degraded_ext:   {degraded_count}")
print()

if signal_counts:
    print("- signal_counts (dedup):")
    for k, v in signal_counts.items():
        print(f"  - {k:<12} {v}")
    print()

if signal_by_user:
    print("- signal_counts_by_from_user:")
    for user, cnt in signal_by_user.items():
        parts = ", ".join([f"{k}:{v}" for k, v in cnt.items()])
        print(f"  - {user or '(empty)'} -> {parts}")
    print()

total_transport_observed = sum(transport_none_by_signal.values()) + sum(
    transport_non_none_by_signal.values()
)
print("- transport_presence (raw lines):")
print(f"  - transport_none_lines:    {sum(transport_none_by_signal.values())} / {total_transport_observed} ({pct(sum(transport_none_by_signal.values()), total_transport_observed)})")
if transport_none_by_signal:
    by_sig = ", ".join([f"{k}:{v}" for k, v in transport_none_by_signal.items()])
    print(f"  - none_by_signal:          {by_sig}")
if transport_non_none_by_signal:
    by_sig = ", ".join([f"{k}:{v}" for k, v in transport_non_none_by_signal.items()])
    print(f"  - non_none_by_signal:      {by_sig}")
if room_ids:
    print(f"  - room_ids_seen:           {', '.join(room_ids.keys())}")
if ws_bases:
    print(f"  - signaling_ws_base_seen:  {', '.join(ws_bases.keys())}")
print()

print("- renegotiate_summary:")
print(f"  - offer_ext_count:         {reneg_offer}")
print(f"  - answer_ext_count:        {reneg_answer}")
print()

if ice_typ_counts:
    print("- ice_candidate_type_counts (raw lines):")
    for typ, v in ice_typ_counts.items():
        print(f"  - {typ:<8} {v}")
    print("- ice_candidate_types_by_user:")
    for user, cnt in ice_typ_by_user.items():
        parts = ", ".join([f"{k}:{v}" for k, v in cnt.items()])
        print(f"  - {user or '(empty)'} -> {parts}")
    print()
if suspicious_ips:
    print("- suspicious_candidate_ips:")
    for ip, v in suspicious_ips.items():
        print(f"  - {ip} ({v})")
    print()

print("[Capability]")
print(f"- lines_with_call_id:        {cap_lines}")
if cap_first_ts and cap_last_ts:
    print(f"- call_lines_window_utc:     {cap_first_ts.isoformat()} -> {cap_last_ts.isoformat()}")
if cap_dispatch:
    print("- dispatch_counts_in_window:")
    for k, v in cap_dispatch.items():
        print(f"  - {k:<24} {v}")
if cap_events:
    print("- sfu_events_with_call_id:")
    for k, v in cap_events.items():
        print(f"  - {k:<24} {v}")
if cap_room_ids:
    print(f"- room_ids_seen:             {', '.join(cap_room_ids.keys())}")
if cap_user_ids:
    print(f"- user_ids_seen:             {', '.join(cap_user_ids.keys())}")
print()

print("[Diagnosis]")
reasons = []
total_none = sum(transport_none_by_signal.values())
if raw_lines > 0:
    if total_transport_observed > 0 and total_none / total_transport_observed > 0.8:
        reasons.append(
            "CallSignal transport 大量为 None：优先检查 MESSAGE_ORCHESTRATOR_CAPABILITY_RTC_BRIDGE 是否开启。"
        )
    if signal_counts.get("Renegotiate", 0) == 0:
        reasons.append("缺少 Renegotiate（SDP）事件：优先检查客户端 onCallSignal 分发与 p2pWebRtc 协商触发。")
    if signal_counts.get("IceCandidate", 0) == 0:
        reasons.append("缺少 IceCandidate 事件：优先检查 onicecandidate 发送链路与对端下行消费。")
    if "host" in ice_typ_counts and suspicious_ips:
        reasons.append(
            "存在可疑 host candidate（如 26.26.x / 169.254.x）：建议优先 relay 或过滤异常网卡。"
        )
    if cap_events.get("CallStarted", 0) > 0 and cap_events.get("MemberJoined", 0) > 1 and signal_counts.get("Renegotiate", 0) > 0:
        reasons.append("SFU/能力侧已创建房间并有双端入会，问题更可能在客户端媒体播放/自动播放策略/ICE质量。")

if raw_lines == 0 and cap_lines == 0:
    reasons.append(
        "当前日志中未命中该 call_id。请先完成一轮通话再执行，或确认传入的 call_id 与 logs 目录正确。"
    )

if reasons:
    for i, r in enumerate(reasons, 1):
        print(f"{i}. {r}")
else:
    print("1. 未发现明显结构性异常（建议结合客户端控制台 WebRTC 状态继续定位）。")

print()
print("[Quick Commands]")
print(f"- tail orchestrator: tail -f {orch_path}")
if cap_path.exists():
    print(f"- tail capability:   tail -f {cap_path}")
print(f"- rerun analyzer:    {analyzer_hint} {call_id}")
PY
