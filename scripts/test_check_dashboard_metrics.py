"""check_dashboard_metrics.sh 的回归测试：真实面板必须通过，各类错误必须被拦下。

每个用例把 deploy 下的面板与告警规则复制到临时目录再改坏，经环境变量指给门禁，
不改仓库里的文件。依赖同级仓库 ../flare-server-core 与 ../flare-core（与门禁本身一致）。
"""

import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check_dashboard_metrics.sh"
DASHBOARDS = ROOT / "deploy/grafana/provisioning/dashboards"
ALERT_RULES = ROOT / "deploy/alert_rules.yml"


class DashboardMetricsGateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="dashboard-gate-"))
        self.addCleanup(shutil.rmtree, self.tmp)
        self.dashboards = self.tmp / "dashboards"
        shutil.copytree(DASHBOARDS, self.dashboards)
        self.alert_rules = self.tmp / "alert_rules.yml"
        shutil.copy(ALERT_RULES, self.alert_rules)

    def run_gate(self, **extra_env):
        env = dict(os.environ)
        env["FLARE_DASHBOARD_DIR"] = str(self.dashboards)
        env["FLARE_ALERT_RULES"] = str(self.alert_rules)
        env.update(extra_env)
        return subprocess.run(
            ["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=120
        )

    def board(self, name):
        return self.dashboards / "flare-im" / name

    def add_panel_query(self, expr):
        path = self.board("system-overview.json")
        board = json.loads(path.read_text())
        board["panels"].append(
            {
                "id": 999,
                "type": "timeseries",
                "title": "injected",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 99},
                "targets": [{"refId": "A", "expr": expr}],
            }
        )
        path.write_text(json.dumps(board, ensure_ascii=False))

    def add_alert(self, expr):
        with self.alert_rules.open("a") as handle:
            handle.write(
                "      - alert: InjectedByTest\n"
                "        expr: |\n"
                f"          {expr}\n"
                "        for: 1m\n"
            )

    def assert_rejected(self, result, *needles):
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        for needle in needles:
            self.assertIn(needle, result.stderr)

    def test_repository_dashboards_and_alerts_pass(self):
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("5 个面板", result.stdout)

    def test_unregistered_metric_in_panel_is_rejected(self):
        self.add_panel_query("sum(rate(messages_sent_total[$__rate_interval]))")
        self.assert_rejected(self.run_gate(), "指标 messages_sent_total 未在源码中注册")

    def test_unregistered_metric_in_alert_rule_is_rejected_with_suggestion(self):
        self.add_alert('sum(rate(http_requests_total{status=~"5.."}[5m])) > 1')
        self.assert_rejected(
            self.run_gate(),
            "指标 http_requests_total 未在源码中注册（相近的注册名：http_server_requests_total）",
        )

    def test_histogram_suffix_only_accepted_for_histograms(self):
        self.add_panel_query("sum(rate(push_success_total_bucket[5m]))")
        self.assert_rejected(self.run_gate(), "指标 push_success_total_bucket 未在源码中注册")

    def test_unknown_selector_label_is_rejected(self):
        self.add_alert('sum(rate(mq_process_ack_total{action=~"nack|term"}[5m])) > 5')
        self.assert_rejected(self.run_gate(), "mq_process_ack_total 没有标签 action")

    def test_grouping_label_missing_from_all_metrics_is_rejected(self):
        self.add_panel_query(
            "histogram_quantile(0.99, sum by (le, tenant_id) "
            "(rate(messages_persisted_duration_seconds_bucket[5m])))"
        )
        self.assert_rejected(self.run_gate(), "分组标签 tenant_id 不属于表达式引用的任何指标")

    def test_api_wrapped_dashboard_is_rejected(self):
        path = self.board("release-gates.json")
        path.write_text(json.dumps({"dashboard": json.loads(path.read_text())}))
        self.assert_rejected(self.run_gate(), "release-gates.json: 顶层必须就是 dashboard")

    def test_duplicate_uid_is_rejected(self):
        source = json.loads(self.board("release-gates.json").read_text())
        target_path = self.board("push-server.json")
        target = json.loads(target_path.read_text())
        target["uid"] = source["uid"]
        target_path.write_text(json.dumps(target, ensure_ascii=False))
        self.assert_rejected(self.run_gate(), "uid flare-im-release-gates 与 flare-im/push-server.json 重复")

    def test_names_in_comments_or_test_modules_do_not_count_as_registered(self):
        fake_core = self.tmp / "flare-core"
        (fake_core / "src").mkdir(parents=True)
        (fake_core / "src/lib.rs").write_text(
            "fn real() {\n"
            '    let c = IntCounterVec::new(Opts::new("gate_probe_real_total", "help; with semicolon"),\n'
            '        &["zone"]).expect("static");\n'
            "    REGISTRY.register(Box::new(c.clone())).expect(\"once\");\n"
            "}\n"
            '// let c = IntCounter::new("gate_probe_commented_total", "help");\n'
            "#[cfg(test)]\n"
            "mod tests {\n"
            "    fn probe() {\n"
            '        let c = IntCounter::new("gate_probe_test_only_total", "help").expect("static");\n'
            "        REGISTRY.register(Box::new(c)).expect(\"once\");\n"
            "    }\n"
            "}\n"
        )
        self.add_panel_query(
            'sum(rate(gate_probe_real_total{zone="a"}[5m])) + sum(rate(gate_probe_commented_total[5m]))'
            " + sum(rate(gate_probe_test_only_total[5m]))"
        )
        result = self.run_gate(FLARE_CORE_DIR=str(fake_core))
        self.assert_rejected(
            result,
            "指标 gate_probe_commented_total 未在源码中注册",
            "指标 gate_probe_test_only_total 未在源码中注册",
        )
        self.assertNotIn("指标 gate_probe_real_total 未在源码中注册", result.stderr)
        self.assertNotIn("没有标签 zone", result.stderr)

    def test_missing_sibling_repository_is_an_error_not_a_pass(self):
        result = self.run_gate(FLARE_SERVER_CORE_DIR=str(self.tmp / "missing"))
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("flare-server-core", result.stderr)


if __name__ == "__main__":
    unittest.main()
