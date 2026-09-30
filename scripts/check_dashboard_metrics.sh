#!/usr/bin/env bash
# 面板/告警指标门禁：Grafana 面板与 Prometheus 告警引用的每个指标，都必须是源码真的注册过的。
#
# 为什么要管：面板和告警里的指标名只是字符串，编译器不管、测试不管、Grafana 也不报错。
# 本仓的五个面板与 alert_rules.yml 曾整片引用从未注册过的指标——messages_sent_total、
# wal_write_*、jetstream_produce_*、online_push_*、push_latency_seconds（真名是
# access_gateway_push_latency_seconds）、http_requests_total（真名是 http_server_requests_total）、
# process_cpu_seconds_total……面板恒为 No data，告警永远不会触发。平时看着一片安静，
# 出了事才发现监控是空的。标签写错同样安静：mq_process_ack_total 的标签是 operation，
# 面板写成 action，查询合法、结果永远为空。
#
# 判据：
# 1. 取出 deploy/grafana/provisioning/dashboards 下全部面板 JSON 与 deploy/alert_rules.yml 里的
#    PromQL 指标名；_bucket/_sum/_count 只有在基名是直方图时才认。
# 2. 注册名取自 Core、flare-server-core、flare-core 源码中的指标构造（Opts::new、HistogramOpts::new、
#    IntCounter::new 等），只认调用了 register 的文件，并剥掉注释与 #[cfg(test)] 模块——
#    注释或测试里出现的名字不算数。另认 Prometheus 为每个抓取目标生成的 up/scrape_*；
#    service-kit 打开 prometheus 的 process 特性并合并默认注册表时，再认 Linux 上的 process_* 进程指标；
#    注册在默认注册表里的指标也只有在这种合并存在时才算有出口。
# 3. 选择器里的标签（metric{label=...}）必须是该指标注册的标签、直方图 _bucket 的 le，
#    或抓取目标标签（job、instance 与 prometheus.yml 的 labels，冲突时的 exported_*）；
#    by/without/on/ignoring 里的标签至少要落在同一表达式引用的某个指标上。
# 4. 面板必须是 Grafana 文件 provisioning 的格式：顶层就是 dashboard，有 title 与唯一 uid。
#    包一层 {"dashboard": ...}（HTTP API 的格式）时 Grafana 拒载，面板根本不会出现。
#
# 源码注册 ≠ 运行时有样本：这里只保证"名字与标签真实存在"。有没有写入路径看 check_dead_metrics.sh，
# 真实流量下有没有样本看 release/scripts/verify_metrics_have_data.sh。
#
# 依赖同级仓库 ../flare-server-core 与 ../flare-core（可用 FLARE_SERVER_CORE_DIR / FLARE_CORE_DIR 覆盖）；
# 缺仓时报错退出而不是跳过——跳过等于没接。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "$ROOT" <<'PY'
import difflib
import json
import os
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
env = os.environ
dash_dir = pathlib.Path(env.get("FLARE_DASHBOARD_DIR", root / "deploy/grafana/provisioning/dashboards"))
alert_file = pathlib.Path(env.get("FLARE_ALERT_RULES", root / "deploy/alert_rules.yml"))
prom_file = pathlib.Path(env.get("FLARE_PROMETHEUS_CONFIG", root / "deploy/prometheus.yml"))
source_roots = [
    ("flare-im-core", root),
    ("flare-server-core", pathlib.Path(env.get("FLARE_SERVER_CORE_DIR", root.parent / "flare-server-core"))),
    ("flare-core", pathlib.Path(env.get("FLARE_CORE_DIR", root.parent / "flare-core"))),
]


def fatal(message):
    print(f"✗ {message}", file=sys.stderr)
    raise SystemExit(2)


for name, path in source_roots:
    if not path.is_dir():
        fatal(f"找不到同级仓库 {name}（{path}）。它注册的指标也会被面板引用，缺仓时无法判定，"
              f"请按开发布局克隆，或用环境变量指到实际位置。")
for path in (dash_dir, alert_file, prom_file):
    if not path.exists():
        fatal(f"找不到 {path}")

# ---------------------------------------------------------------------------
# 源码里的注册名
# ---------------------------------------------------------------------------
SKIP_DIRS = {"target", "tests", "benches", "examples", "node_modules"}


def rust_files(base):
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".")]
        for filename in filenames:
            if filename.endswith(".rs"):
                yield pathlib.Path(dirpath) / filename


def skip_string(text, i):
    """i 指向开引号，返回闭引号之后的位置。"""
    n = len(text)
    j = i + 1
    while j < n and text[j] != '"':
        j += 2 if text[j] == "\\" else 1
    return j + 1


def strip_comments(text):
    """去掉 // 与 /* */ 注释，保留字符串与字符字面量。"""
    out, i, n = [], 0, len(text)
    while i < n:
        c = text[i]
        if c == '"':
            j = skip_string(text, i)
            out.append(text[i:j])
            i = j
        elif c == "'" and i + 2 < n and text[i + 1] != "\\" and text[i + 2] == "'":
            out.append(text[i:i + 3])
            i += 3
        elif c == "'" and i + 1 < n and text[i + 1] == "\\":
            j = text.find("'", i + 2)
            j = n if j < 0 else j + 1
            out.append(text[i:j])
            i = j
        elif text.startswith("//", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            i = n if j < 0 else j + 2
        else:
            out.append(c)
            i += 1
    return "".join(out)


TEST_MODULE = re.compile(r"#\[cfg\(test\)\]\s*(?:#\[[^\]]*\]\s*)*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*\{")


def strip_test_modules(text):
    while True:
        m = TEST_MODULE.search(text)
        if not m:
            return text
        depth, i, n = 1, m.end(), len(text)
        while i < n and depth:
            c = text[i]
            if c == '"':
                i = skip_string(text, i)
                continue
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
            i += 1
        text = text[:m.start()] + text[i:]


CONSTRUCTOR = re.compile(
    r"\b(?:(?P<kind>HistogramOpts|Opts|IntCounter|IntGauge|UIntCounter|UIntGauge|Counter|Gauge)::new"
    r"|(?P<macro>register_\w+|histogram_opts|opts)!)\(\s*\"(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)\""
)
LABEL_ARRAY = re.compile(r"&\[\s*((?:\"[^\"]*\"\s*,?\s*)*)\]")
RUST_STRING = re.compile(r'"(?:[^"\\]|\\.)*"')
REGISTERS = re.compile(r"\bregister\s*\(|\bregister_\w+!\s*\(")


class Metric:
    def __init__(self, kind, labels, origin, via_default_registry=False):
        self.kind = kind
        self.labels = labels
        self.origin = origin
        self.via_default_registry = via_default_registry


registered = {}
for repo, base in source_roots:
    for path in rust_files(base):
        raw = path.read_text(errors="replace")
        if "register" not in raw:
            continue
        text = strip_test_modules(strip_comments(raw))
        if not REGISTERS.search(text):
            continue
        via_default = "default_registry()" in text
        for m in CONSTRUCTOR.finditer(text):
            window = text[m.end():m.end() + 800]
            # 找构造语句的结尾时要屏蔽字符串：help 文本里的分号（"identity; value is always 1"）
            # 曾让标签数组被截掉，flare_service_info 被当成无标签指标。
            masked = RUST_STRING.sub(lambda s: '"' + " " * (len(s.group()) - 2) + '"', window)
            cut = min([pos for pos in (masked.find(".expect("), masked.find("?;"), masked.find(";"))
                       if pos >= 0] or [len(window)])
            labels_match = LABEL_ARRAY.search(window[:cut])
            labels = set(re.findall(r"\"([^\"]*)\"", labels_match.group(1))) if labels_match else set()
            macro = m.group("macro") or ""
            is_histogram = m.group("kind") == "HistogramOpts" or "histogram" in macro
            origin = f"{repo}/{path.relative_to(base)}"
            registered.setdefault(
                m.group("name"),
                Metric("histogram" if is_histogram else "scalar", labels, origin, via_default),
            )

# 默认注册表只有在 service-kit 合并它时才有出口；进程指标也来自那里（prometheus 的 process 特性，仅 Linux）。
service_kit = root / "crates/flare-im-service-kit"
kit_manifest = (service_kit / "Cargo.toml").read_text()
kit_metrics = strip_test_modules(strip_comments((service_kit / "src/metrics/mod.rs").read_text()))
merges_default_registry = "prometheus::default_registry()" in kit_metrics
process_feature = re.search(r"^prometheus\s*=\s*\{[^}]*features\s*=\s*\[[^\]]*\"process\"", kit_manifest, re.M)
unexported = set()
if not merges_default_registry:
    unexported = {k for k, v in registered.items() if v.via_default_registry}
    registered = {k: v for k, v in registered.items() if k not in unexported}
if merges_default_registry and process_feature:
    for name in ("process_cpu_seconds_total", "process_open_fds", "process_max_fds",
                 "process_virtual_memory_bytes", "process_resident_memory_bytes",
                 "process_start_time_seconds", "process_threads"):
        registered.setdefault(name, Metric("scalar", set(), "prometheus ProcessCollector（默认注册表，Linux）"))

# Prometheus 为每个抓取目标自动生成的序列。
for name in ("up", "scrape_duration_seconds", "scrape_samples_scraped",
             "scrape_samples_post_metric_relabeling", "scrape_series_added"):
    registered.setdefault(name, Metric("scalar", set(), "Prometheus 抓取目标"))

if len(registered) < 30:
    fatal(f"只从源码里找到 {len(registered)} 个注册指标，源码扫描大概率失效（目录结构变了？），拒绝放行。")

# 抓取目标标签：job、instance 与 prometheus.yml 里 static_configs 的 labels。
target_labels = {"job", "instance"}
prom_lines = prom_file.read_text().splitlines()
for idx, line in enumerate(prom_lines):
    m = re.match(r"^(\s*)labels:\s*$", line)
    if not m:
        continue
    indent = len(m.group(1))
    for follow in prom_lines[idx + 1:]:
        if not follow.strip():
            continue
        if len(follow) - len(follow.lstrip()) <= indent:
            break
        key = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", follow)
        if key:
            target_labels.add(key.group(1))

# ---------------------------------------------------------------------------
# PromQL 分析
# ---------------------------------------------------------------------------
KEYWORDS = {"and", "or", "unless", "bool", "offset", "by", "without", "on", "ignoring",
            "group_left", "group_right", "inf", "nan", "atan2", "start", "end"}
STRING = re.compile(r'"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'|`[^`]*`')
GRAFANA_VAR = re.compile(r"\$\{[^}]*\}|\$__\w+|\$\w+")
SELECTOR = re.compile(r"(?<![\w:.])([a-zA-Z_:][a-zA-Z0-9_:]*)?\s*\{([^}]*)\}")
MATCHER_LABEL = re.compile(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=~|!~|!=|=)")
GROUPING = re.compile(r"\b(by|without|on|ignoring|group_left|group_right)\s*\(([^)]*)\)")
IDENT = re.compile(r"(?<![\w:.])([a-zA-Z_:][a-zA-Z0-9_:]*)(\s*\()?")


def resolve(name):
    """返回 (Metric, 是否直方图 _bucket)；未注册返回 (None, False)。"""
    if name in registered:
        return registered[name], False
    for suffix in ("_bucket", "_sum", "_count"):
        if name.endswith(suffix):
            base = registered.get(name[: -len(suffix)])
            if base is not None and base.kind == "histogram":
                return base, suffix == "_bucket"
    return None, False


def allowed_labels(metric, is_bucket):
    labels = set(metric.labels) | target_labels | {"__name__"}
    labels |= {f"exported_{label}" for label in metric.labels if label in target_labels}
    if is_bucket:
        labels.add("le")
    return labels


def suggest(name):
    close = difflib.get_close_matches(name, registered.keys(), n=1, cutoff=0.6)
    return f"（相近的注册名：{close[0]}）" if close else ""


def analyze(expr):
    """返回问题列表与引用到的指标名。"""
    problems = []
    text = STRING.sub('""', expr)
    text = GRAFANA_VAR.sub(" 0 ", text)
    text = re.sub(r"\[[^\]]*\]", " ", text)

    selector_labels = {}

    def take_selector(m):
        name = m.group(1)
        labels = MATCHER_LABEL.findall(m.group(2))
        if name and name not in KEYWORDS:
            selector_labels.setdefault(name, set()).update(labels)
            return f" {name} "
        if "__name__" not in labels:
            problems.append(f"裸选择器 {{{m.group(2).strip()}}} 没有指标名，无法校验")
        return " "

    text = SELECTOR.sub(take_selector, text)

    grouping = []

    def take_grouping(m):
        grouping.extend(label.strip() for label in m.group(2).split(",") if label.strip())
        return " "

    text = GROUPING.sub(take_grouping, text)

    referenced = []
    for m in IDENT.finditer(text):
        token, call = m.group(1), m.group(2)
        if call or token in KEYWORDS:
            continue
        referenced.append(token)
    referenced += [name for name in selector_labels if name not in referenced]

    union = set()
    for name in dict.fromkeys(referenced):
        metric, is_bucket = resolve(name)
        if metric is None:
            if name in unexported:
                problems.append(f"指标 {name} 注册在 prometheus 默认注册表，但 service-kit 的 /metrics 没有合并默认注册表，无出口")
            else:
                problems.append(f"指标 {name} 未在源码中注册{suggest(name)}")
            continue
        allowed = allowed_labels(metric, is_bucket)
        union |= allowed
        for label in sorted(selector_labels.get(name, ())):
            if label not in allowed:
                problems.append(f"指标 {name} 没有标签 {label}（注册的标签：{sorted(metric.labels) or '无'}）")
    if union:
        for label in dict.fromkeys(grouping):
            if label not in union:
                problems.append(f"分组标签 {label} 不属于表达式引用的任何指标")
    if not referenced:
        problems.append("表达式里没有找到任何指标名")
    return problems, referenced


failures = []
query_count = 0
used = set()


def check_expr(where, expr):
    global query_count
    query_count += 1
    problems, referenced = analyze(expr)
    used.update(referenced)
    for problem in problems:
        failures.append(f"{where}: {problem}\n      {' '.join(expr.split())}")


# ---------------------------------------------------------------------------
# 面板
# ---------------------------------------------------------------------------
dashboards = sorted(dash_dir.rglob("*.json"))
if not dashboards:
    fatal(f"{dash_dir} 下没有任何面板 JSON——门禁扫不到东西比报错更危险。")

uids = {}


def walk(node, where):
    if isinstance(node, dict):
        title = node.get("title")
        here = f"{where} / {title}" if isinstance(title, str) and title and node.get("type") else where
        expr = node.get("expr")
        if isinstance(expr, str) and expr.strip():
            check_expr(f"{here} [{node.get('refId', '?')}]", expr)
        for key, value in node.items():
            if key != "expr":
                walk(value, here)
    elif isinstance(node, list):
        for item in node:
            walk(item, where)


for path in dashboards:
    rel = path.relative_to(dash_dir)
    try:
        board = json.loads(path.read_text())
    except json.JSONDecodeError as error:
        failures.append(f"{rel}: 不是合法 JSON：{error}")
        continue
    if not isinstance(board, dict) or "dashboard" in board:
        failures.append(f"{rel}: 顶层必须就是 dashboard；包一层 {{\"dashboard\": ...}} 是 HTTP API 的格式，"
                        f"文件 provisioning 会以 title 为空拒载")
        continue
    if not str(board.get("title", "")).strip():
        failures.append(f"{rel}: 缺少 title")
    uid = str(board.get("uid", "")).strip()
    if not uid:
        failures.append(f"{rel}: 缺少 uid（没有 uid 时每次重载都换链接，告警与书签全断）")
    elif uid in uids:
        failures.append(f"{rel}: uid {uid} 与 {uids[uid]} 重复")
    else:
        uids[uid] = rel
    walk(board.get("panels", []), str(rel))
    for variable in board.get("templating", {}).get("list", []):
        query = variable.get("query")
        if isinstance(query, dict):
            query = query.get("query")
        if variable.get("type") == "query" and isinstance(query, str):
            m = re.match(r"^\s*label_values\(\s*(.+?)\s*,\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s*$", query)
            if m:
                check_expr(f"{rel} / 变量 {variable.get('name')}", f"count({m.group(1)}) by ({m.group(2)})")
            elif query.strip().startswith("query_result("):
                check_expr(f"{rel} / 变量 {variable.get('name')}", query.strip()[len("query_result("):-1])

# ---------------------------------------------------------------------------
# 告警规则（只解析 rule 的 alert/record 与 expr，不依赖 PyYAML）
# ---------------------------------------------------------------------------
rule_count, rule_exprs = 0, 0
current = "?"
lines = alert_file.read_text().splitlines()
i = 0
while i < len(lines):
    line = lines[i]
    m = re.match(r"^\s*-\s+(?:alert|record):\s*(.+?)\s*$", line)
    if m:
        rule_count += 1
        current = m.group(1).strip("'\"")
    m = re.match(r"^(\s*)(?:-\s+)?expr:\s*(.*?)\s*$", line)
    if m:
        indent, rest = len(m.group(1)), m.group(2)
        if rest in ("|", "|-", "|+", ">", ">-", ">+"):
            block = []
            i += 1
            while i < len(lines) and (not lines[i].strip() or len(lines[i]) - len(lines[i].lstrip()) > indent):
                block.append(lines[i].strip())
                i += 1
            expr = " ".join(part for part in block if part)
        else:
            expr = rest[1:-1] if len(rest) > 1 and rest[0] == rest[-1] and rest[0] in "'\"" else rest
            i += 1
        rule_exprs += 1
        check_expr(f"{alert_file.name} / {current}", expr)
        continue
    i += 1

if rule_count == 0:
    fatal(f"{alert_file} 里没有解析到任何告警规则——门禁扫不到东西比报错更危险。")
if rule_count != rule_exprs:
    failures.append(f"{alert_file.name}: 解析到 {rule_count} 条规则、{rule_exprs} 个 expr，数量对不上（格式超出本门禁的解析范围？）")

if failures:
    for failure in failures:
        print(f"  ✗ {failure}", file=sys.stderr)
    print("", file=sys.stderr)
    print("面板和告警只能引用源码里真实注册的指标与标签。引用不存在的名字不会报任何错，", file=sys.stderr)
    print("只会让面板恒为 No data、告警永不触发。先 `curl <服务>/metrics | grep '^# TYPE'` 确认真名。", file=sys.stderr)
    raise SystemExit(1)

print(f"✓ {len(dashboards)} 个面板、{rule_count} 条告警规则共 {query_count} 条查询，"
      f"引用的 {len(used)} 个指标都已注册（源码注册 {len(registered)} 个）")
PY
