"""夹具脚本回归：从空库跑 scripts/ledger_demo.py，断言独立核对通过。

这是“原始小夹具逐日判定、再按自然日合并，独立核对整张区间表及来源登记”
的可执行证据；脚本内部不复用台账更新逻辑。
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class FixtureDemoCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bgfix_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_demo_matches_independent_episodes(self):
        out = subprocess.run(
            [sys.executable, os.path.join(ROOT, "scripts", "ledger_demo.py"),
             "--work-dir", os.path.join(self.tmp, "run")],
            capture_output=True, text=True, cwd=ROOT,
            env={**os.environ, "PYTHONPATH": ROOT, "PYTHONWARNINGS": "ignore"},
        )
        self.assertEqual(out.returncode, 0, out.stderr)
        rep = json.loads(out.stdout)
        self.assertTrue(rep["ok"])
        self.assertTrue(rep["sources_match_independent"])
        self.assertEqual(rep["mismatch_episodes"], [])
        # 乱序补报把 03-02 与 03-03 的 A 合并到一段（轨迹第 2 步）
        step2 = rep["trace"][1]["episodes"]
        self.assertIn(["A", "2026-03-02", "2026-03-03", "true"], step2)
        # 历史更正移除 A 后，最终 03-02 与 03-05 不再连续
        self.assertNotIn(["A", "2026-03-02", "2026-03-05", "true"],
                         rep["final_episodes"])
        self.assertIn(["A", "2026-03-02", "2026-03-02", "true"],
                      rep["final_episodes"])
        self.assertIn(["A", "2026-03-05", "2026-03-05", "true"],
                      rep["final_episodes"])
        self.assertEqual(rep["resend_changed_entities"], 0)


class ContrastCase(unittest.TestCase):
    """对照实验：同日更正代价与旧历史规模无关（真实 SQLite 读数）。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bgcontrast_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_contrast_reports_independent_of_history(self):
        out = subprocess.run(
            [sys.executable,
             os.path.join(ROOT, "scripts", "ledger_contrast.py"),
             "--work-dir", os.path.join(self.tmp, "run")],
            capture_output=True, text=True, cwd=ROOT,
            env={**os.environ, "PYTHONPATH": ROOT, "PYTHONWARNINGS": "ignore"},
        )
        self.assertEqual(out.returncode, 0, out.stderr)
        rep = json.loads(out.stdout)
        self.assertTrue(
            rep["conclusion"]["unrelated_history_does_not_change_update_cost"])
        self.assertTrue(
            rep["conclusion"][
                "target_segment_length_does_not_change_update_cost"])


if __name__ == "__main__":
    unittest.main()
