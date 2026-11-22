"""复核包导出的端到端测试。

覆盖题目验收点：

* 小样本手算：三态、优先级摘要序、整组成本；与独立实现逐项核对输出；
* 装不下的高优先级组被跳过、后续候选补位；
* 子记录额度为零时仍可选中无子记录对象；
* 导入行序变化不改变中选名单与输出字节；
* 规则变化后同一对象改占另一类名额；
* 发布前写入失败：返回非零、最终目录不存在、无暂存残留；
* 输出路径已存在拒绝覆盖；空包三份文件；
* 属性保真（null / 空串 / bool / int 互不混淆），整组导出（含不满足
  规则的子记录），落选对象不出现，组内 child_id UTF-8 字节序；
* manifest 字段；
* 导出阶段只恢复中选对象及其子组属性（可观察计数）。

运行：``PYTHONPATH=/workspace python3 -m unittest tests.test_review -v``
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from bitgroup import review as review_mod
from bitgroup.engine import Engine
from bitgroup.errors import ReviewError
from bitgroup.importer import Importer
from bitgroup.review import (
    export_review_package,
    plan_from_rows,
    priority_digest,
)

PS = {"region": "text", "level": "int64", "flag": "bool", "note": "null"}
CS = {"temp": "int64", "source": "text", "ok": "bool", "tag": "null"}

PARENT_ROWS = [
    {"entity_id": "P", "region": "N", "level": None, "flag": True, "note": None},
    {"entity_id": "Q", "region": "N", "level": 2, "flag": False, "note": None},
    {"entity_id": "R", "region": "S", "level": 3, "flag": None, "note": None},
    {"entity_id": "S", "region": "", "level": 1, "flag": True, "note": None},
    {"entity_id": "T", "region": "N", "level": 0, "flag": False, "note": None},
]
CHILD_ROWS = [
    {"child_id": "p1", "entity_id": "P", "temp": 20, "source": "Y",
     "ok": True, "tag": None},
    {"child_id": "p2", "entity_id": "P", "temp": None, "source": "X",
     "ok": False, "tag": None},
    {"child_id": "q1", "entity_id": "Q", "temp": 20, "source": "X",
     "ok": True, "tag": None},
    {"child_id": "q2", "entity_id": "Q", "temp": 21, "source": "X",
     "ok": None, "tag": None},
    {"child_id": "s1", "entity_id": "S", "temp": 30, "source": "Z",
     "ok": False, "tag": None},
    {"child_id": "s2", "entity_id": "S", "temp": 31, "source": "Z",
     "ok": True, "tag": None},
    # T 无子记录
]

AST = {
    "op": "and",
    "args": [
        {"op": "cmp", "domain": "parent", "attr": "region",
         "op_kind": "eq", "value": "N"},
        {"op": "exists", "arg": {"op": "and", "args": [
            {"op": "cmp", "domain": "child", "attr": "temp",
             "op_kind": "lt", "value": 25},
            {"op": "cmp", "domain": "child", "attr": "source",
             "op_kind": "eq", "value": "X"},
        ]}},
    ],
}

# 参考实现的判定（与 test_engine 的手算一致：P unknown / Q true /
# R false / S false / T false）
REF_STATES = {"P": "unknown", "Q": "true", "R": "false",
              "S": "false", "T": "false"}


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def read_jsonl(path):
    out = []
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if raw:
                out.append(json.loads(raw))
    return out


def children_by_entity(rows):
    out = {}
    for r in rows:
        out.setdefault(r["entity_id"], []).append(r)
    return out


def attrs_of(row, keys):
    return {k: row[k] for k in keys}


class ReviewCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bgreview_")
        self.pp = os.path.join(self.tmp, "parent.jsonl")
        self.cp = os.path.join(self.tmp, "child.jsonl")
        write_jsonl(self.pp, PARENT_ROWS)
        write_jsonl(self.cp, CHILD_ROWS)
        self.idx = os.path.join(self.tmp, "idx")
        Importer(self.idx, PS, CS).run(self.pp, self.cp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ---- 辅助 ----------------------------------------------------------

    def export(self, out, **kw):
        params = dict(
            seed="seed-1",
            quota_true=5,
            quota_false=5,
            quota_unknown=5,
            max_children=1000,
        )
        params.update(kw)
        return export_review_package(self.idx, AST, out_dir=out, **params)

    def expected_plan(self, quota_true=5, quota_false=5, quota_unknown=5,
                      **kw):
        params = dict(seed="seed-1",
                      quotas={"true": quota_true, "false": quota_false,
                              "unknown": quota_unknown},
                      max_children=1000)
        params.update(kw)
        return plan_from_rows(PARENT_ROWS, children_by_entity(CHILD_ROWS),
                              REF_STATES, **params)

    def assert_package_matches_plan(self, out, plan):
        parents = read_jsonl(os.path.join(out, "parents.jsonl"))
        children = read_jsonl(os.path.join(out, "children.jsonl"))
        selected_ids = [r["entity_id"] for r in plan["selected"]]
        self.assertEqual([r["entity_id"] for r in parents], selected_ids)
        # 组顺序与 parents 相同；无子记录的中选对象不出现在 children
        with_children = set(children_by_entity(CHILD_ROWS))
        seen = []
        for row in children:
            if not seen or seen[-1] != row["entity_id"]:
                seen.append(row["entity_id"])
        self.assertEqual(seen, [e for e in selected_ids
                                if e in with_children])
        return parents, children

    # ---- 小样本手算 ----------------------------------------------------

    def test_priority_is_sha256_of_seed_nul_entity(self):
        want = sorted(
            REF_STATES,
            key=lambda e: (priority_digest("seed-1", e), e.encode("utf-8")))
        out = os.path.join(self.tmp, "pkg")
        res = self.export(out)
        got = [r["entity_id"] for r in
               read_jsonl(os.path.join(out, "parents.jsonl"))]
        # 全部额度足够时，中选名单就是完整优先级序
        self.assertEqual(got, want)
        self.assertEqual(res.manifest["n_parents"], len(PARENT_ROWS))

    def test_matches_independent_plan_and_attributes(self):
        plan = self.expected_plan(quota_true=0, quota_false=1,
                                  quota_unknown=1, max_children=3)
        out = os.path.join(self.tmp, "pkg")
        res = self.export(out, quota_true=0, quota_false=1,
                          quota_unknown=1, max_children=3)
        parents, children = self.assert_package_matches_plan(out, plan)

        # 属性恢复业务值：显式 null / 空串 / bool / int 保持原区别
        by_id = {p["entity_id"]: p for p in PARENT_ROWS}
        cbs = children_by_entity(CHILD_ROWS)
        for row in parents:
            src = by_id[row["entity_id"]]
            self.assertEqual(row["state"], REF_STATES[row["entity_id"]])
            self.assertEqual(row["attributes"],
                             attrs_of(src, PS.keys()))
            self.assertIsInstance(row["attributes"]["level"],
                                  (int, type(None)))
            if row["attributes"]["flag"] is not None:
                self.assertIsInstance(row["attributes"]["flag"], bool)
        for row in children:
            src = next(c for c in cbs[row["entity_id"]]
                       if c["child_id"] == row["child_id"])
            self.assertEqual(row["attributes"], attrs_of(src, CS.keys()))
        # 落选对象不在任一文件
        selected = {p["entity_id"] for p in parents}
        for row in children:
            self.assertIn(row["entity_id"], selected)
            self.assertNotEqual(row["entity_id"], "S")
        # manifest
        m = res.manifest
        with Engine(self.idx) as eng:
            self.assertEqual(m["index_token"], eng.index_token)
            self.assertEqual(
                m["ast_fingerprint"],
                eng.counts(AST)["fingerprint"])
        self.assertEqual(m["seed"], "seed-1")
        self.assertEqual(m["quotas"],
                         {"true": 0, "false": 1, "unknown": 1})
        self.assertEqual(m["max_children"], 3)
        self.assertEqual(m["candidates"], {
            "true": 1, "false": 3, "unknown": 1})
        self.assertEqual(m["n_children"],
                         sum(len(cbs.get(e, [])) for e in selected))
        on_disk = json.load(open(os.path.join(out, "manifest.json"),
                                 encoding="utf-8"))
        self.assertEqual(on_disk, m)

    def test_exports_all_children_including_rule_mismatch(self):
        # P 中选；p1 不满足规则（source=Y），也必须整组带出
        plan = self.expected_plan(seed="pick-P", quota_true=0,
                                  quota_false=0, quota_unknown=1,
                                  max_children=10)
        out = os.path.join(self.tmp, "pkg")
        self.export(out, seed="pick-P", quota_true=0, quota_false=0,
                    quota_unknown=1, max_children=10)
        parents, children = self.assert_package_matches_plan(out, plan)
        self.assertEqual([p["entity_id"] for p in parents], ["P"])
        self.assertEqual(sorted(c["child_id"] for c in children),
                         ["p1", "p2"])

    def test_children_group_sorted_by_utf8_bytes(self):
        # 用带多字节/前缀关系的 child_id 验证字节序
        rows = [
            {"child_id": "z", "entity_id": "A", "temp": 1, "source": "X",
             "ok": True, "tag": None},
            {"child_id": "ä", "entity_id": "A", "temp": 2, "source": "X",
             "ok": True, "tag": None},
            {"child_id": "aa", "entity_id": "A", "temp": 3, "source": "X",
             "ok": True, "tag": None},
            {"child_id": "a", "entity_id": "A", "temp": 4, "source": "X",
             "ok": True, "tag": None},
        ]
        pp = os.path.join(self.tmp, "ap.jsonl")
        cp = os.path.join(self.tmp, "ac.jsonl")
        write_jsonl(pp, [{"entity_id": "A", "region": "N", "level": 1,
                          "flag": True, "note": None}])
        write_jsonl(cp, rows)
        idx = os.path.join(self.tmp, "aidx")
        Importer(idx, PS, CS).run(pp, cp)
        out = os.path.join(self.tmp, "apkg")
        export_review_package(idx, AST, seed="s", quota_true=1,
                              quota_false=0, quota_unknown=1,
                              max_children=10, out_dir=out)
        got = [r["child_id"] for r in
               read_jsonl(os.path.join(out, "children.jsonl"))]
        want = sorted([r["child_id"] for r in rows],
                      key=lambda s: s.encode("utf-8"))
        self.assertEqual(got, want)
        self.assertEqual(got, ["a", "aa", "z", "ä"])

    # ---- 整组成本与补位 ------------------------------------------------

    def test_high_priority_group_skipped_and_backfilled(self):
        # 构造：BIG 组 4 条子记录且规则 true、优先级最高；其它 true 组 1 条。
        # 额度只够 1 条子记录 → BIG 装不下被跳过，后面的小组补位。
        parents = [{"entity_id": e, "region": "N", "level": i,
                    "flag": True, "note": None}
                   for i, e in enumerate(["BIG", "S1", "S2"])]
        children = []
        for i in range(4):
            children.append({"child_id": f"big{i}", "entity_id": "BIG",
                             "temp": 10, "source": "X", "ok": True,
                             "tag": None})
        for e in ("S1", "S2"):
            children.append({"child_id": f"{e}-1", "entity_id": e,
                             "temp": 10, "source": "X", "ok": True,
                             "tag": None})
        pp, cp = os.path.join(self.tmp, "bp.jsonl"), os.path.join(
            self.tmp, "bc.jsonl")
        write_jsonl(pp, parents)
        write_jsonl(cp, children)
        idx = os.path.join(self.tmp, "bidx")
        Importer(idx, PS, CS).run(pp, cp)
        # 找一个让 BIG 排在最前的 seed
        seed = None
        for i in range(200):
            cand = f"seed{i}"
            order = sorted([p["entity_id"] for p in parents],
                           key=lambda e: (priority_digest(cand, e),
                                          e.encode("utf-8")))
            if order[0] == "BIG":
                seed = cand
                break
        self.assertIsNotNone(seed)
        out = os.path.join(self.tmp, "bpkg")
        res = export_review_package(
            idx, AST, seed=seed, quota_true=3, quota_false=0,
            quota_unknown=0, max_children=2, out_dir=out)
        got = [r["entity_id"] for r in
               read_jsonl(os.path.join(out, "parents.jsonl"))]
        # BIG(4) 装不下 → 跳过；后面两个小组各 1 条都装进 2 的额度
        self.assertEqual(got, [e for e in sorted(
            ["S1", "S2"], key=lambda e: (priority_digest(seed, e),
                                         e.encode("utf-8")))])
        self.assertEqual(res.metrics["child_budget_skips"], 1)
        self.assertEqual(res.manifest["selected"]["true"], 2)
        self.assertEqual(res.manifest["n_children"], 2)

    def test_zero_child_budget_still_selects_childless_object(self):
        # T 无子记录；额度 0 仍可入选（成本为零）
        order = sorted(REF_STATES, key=lambda e: (
            priority_digest("s0", e), e.encode("utf-8")))
        out = os.path.join(self.tmp, "pkg")
        res = self.export(out, seed="s0", quota_true=5, quota_false=5,
                          quota_unknown=5, max_children=0)
        got = [r["entity_id"] for r in
               read_jsonl(os.path.join(out, "parents.jsonl"))]
        self.assertIn("T", got)
        # 有子记录的都被零额度挡住
        for e in ("P", "Q", "S"):
            self.assertNotIn(e, got)
        # 中选顺序仍是优先级序的子序列
        self.assertEqual(got, [e for e in order if e in set(got)])
        self.assertEqual(res.manifest["n_children"], 0)

    def test_skipped_object_does_not_block_later_smaller_groups(self):
        parents = [{"entity_id": e, "region": "N", "level": i,
                    "flag": False, "note": None}
                   for i, e in enumerate(["C1", "C2", "C3"])]
        children = [
            {"child_id": "c1-a", "entity_id": "C1", "temp": 1,
             "source": "Z", "ok": True, "tag": None},
            {"child_id": "c2-a", "entity_id": "C2", "temp": 1,
             "source": "Z", "ok": True, "tag": None},
            {"child_id": "c2-b", "entity_id": "C2", "temp": 1,
             "source": "Z", "ok": True, "tag": None},
            {"child_id": "c2-c", "entity_id": "C2", "temp": 1,
             "source": "Z", "ok": True, "tag": None},
            {"child_id": "c3-a", "entity_id": "C3", "temp": 1,
             "source": "Z", "ok": True, "tag": None},
        ]
        pp, cp = os.path.join(self.tmp, "lp.jsonl"), os.path.join(
            self.tmp, "lc.jsonl")
        write_jsonl(pp, parents)
        write_jsonl(cp, children)
        idx = os.path.join(self.tmp, "lidx")
        Importer(idx, PS, CS).run(pp, cp)
        # C2 是 false 且 3 条；C1/C3 各 1 条。额度 2：若先遇 C2 则跳过，
        # C1 与 C3 都可中选（各消耗 1，剩 0）。
        seed = None
        for i in range(500):
            cand = f"z{i}"
            order = sorted(["C1", "C2", "C3"], key=lambda e: (
                priority_digest(cand, e), e.encode("utf-8")))
            if order[0] == "C2":
                seed = cand
                break
        self.assertIsNotNone(seed)
        out = os.path.join(self.tmp, "lpkg")
        res = export_review_package(
            idx, AST, seed=seed, quota_true=0, quota_false=3,
            quota_unknown=0, max_children=2, out_dir=out)
        got = [r["entity_id"] for r in
               read_jsonl(os.path.join(out, "parents.jsonl"))]
        self.assertEqual(sorted(got), ["C1", "C3"])
        self.assertEqual(res.manifest["n_children"], 2)

    # ---- 顺序无关性 ----------------------------------------------------

    def test_import_order_does_not_change_selection_or_bytes(self):
        # 同样的业务数据，主/子行序完全颠倒 → 局部编号改变，但中选名单
        # 与输出必须逐字节一致（manifest 除外，它含索引代号）。
        rev_idx = os.path.join(self.tmp, "rev_idx")
        rp = os.path.join(self.tmp, "rp.jsonl")
        rc = os.path.join(self.tmp, "rc.jsonl")
        write_jsonl(rp, list(reversed(PARENT_ROWS)))
        write_jsonl(rc, list(reversed(CHILD_ROWS)))
        Importer(rev_idx, PS, CS).run(rp, rc)

        out1 = os.path.join(self.tmp, "pkg1")
        out2 = os.path.join(self.tmp, "pkg2")
        self.export(out1, seed="ord", quota_true=1, quota_false=1,
                    quota_unknown=1, max_children=3)
        export_review_package(
            rev_idx, AST, seed="ord", quota_true=1, quota_false=1,
            quota_unknown=1, max_children=3, out_dir=out2)
        for name in ("parents.jsonl", "children.jsonl"):
            with open(os.path.join(out1, name), "rb") as a, \
                    open(os.path.join(out2, name), "rb") as b:
                self.assertEqual(a.read(), b.read(), name)

    # ---- 规则变化 -> 改占另一类名额 -------------------------------------

    def test_rule_change_moves_object_to_other_class_quota(self):
        # P 在 AST 下是 unknown；把规则改成「region==S」后 P（region=N）
        # 变为 false，于是应占 false 名额而不是 unknown 名额。
        ast2 = {"op": "cmp", "domain": "parent", "attr": "region",
                "op_kind": "eq", "value": "S"}
        out1 = os.path.join(self.tmp, "q1")
        self.export(out1, seed="mv", quota_true=0, quota_false=1,
                    quota_unknown=0, max_children=1000)
        p1 = read_jsonl(os.path.join(out1, "parents.jsonl"))
        self.assertNotIn("P", [r["entity_id"] for r in p1])
        self.assertEqual(p1[0]["state"], "false")

        out2 = os.path.join(self.tmp, "q2")
        res2 = export_review_package(
            self.idx, ast2, seed="mv", quota_true=0, quota_false=0,
            quota_unknown=1, max_children=1000, out_dir=out2)
        # 在 region==S 规则下 unknown 名额只可能选中 unknown 类对象
        p2 = read_jsonl(os.path.join(out2, "parents.jsonl"))
        self.assertTrue(all(r["state"] == "unknown" for r in p2))
        self.assertEqual(res2.manifest["candidates"]["true"] +
                         res2.manifest["candidates"]["false"] +
                         res2.manifest["candidates"]["unknown"],
                         len(PARENT_ROWS))

        out3 = os.path.join(self.tmp, "q3")
        res3 = export_review_package(
            self.idx, ast2, seed="mv", quota_true=0, quota_false=1,
            quota_unknown=0, max_children=1000, out_dir=out3)
        p3 = read_jsonl(os.path.join(out3, "parents.jsonl"))
        self.assertTrue(all(r["state"] == "false" for r in p3))
        self.assertEqual(res3.manifest["selected"]["unknown"], 0)

    # ---- 名额为零不改变判定 --------------------------------------------

    def test_zero_quota_does_not_alter_states(self):
        out = os.path.join(self.tmp, "pkg")
        res = self.export(out, quota_true=0, quota_false=0, quota_unknown=0,
                          max_children=0)
        self.assertEqual(res.manifest["candidates"], {
            "true": 1, "false": 3, "unknown": 1})
        self.assertEqual(res.manifest["selected"],
                         {"true": 0, "false": 0, "unknown": 0})
        self.assertEqual(read_jsonl(os.path.join(out, "parents.jsonl")), [])
        self.assertEqual(read_jsonl(os.path.join(out, "children.jsonl")), [])
        # 空包也有三份文件
        self.assertEqual(
            sorted(os.listdir(out)),
            ["children.jsonl", "manifest.json", "parents.jsonl"])

    # ---- 发布与失败 ----------------------------------------------------

    def test_existing_out_path_rejected_untouched(self):
        out = os.path.join(self.tmp, "taken")
        os.makedirs(out)
        marker = os.path.join(out, "keep.txt")
        with open(marker, "w") as fh:
            fh.write("keep")
        with self.assertRaises(ReviewError):
            self.export(out)
        self.assertEqual(os.listdir(out), ["keep.txt"])
        with open(marker) as fh:
            self.assertEqual(fh.read(), "keep")

    def test_existing_out_file_rejected(self):
        out = os.path.join(self.tmp, "afile")
        with open(out, "w") as fh:
            fh.write("x")
        with self.assertRaises(ReviewError):
            self.export(out)
        with open(out) as fh:
            self.assertEqual(fh.read(), "x")

    def test_publish_failure_leaves_no_package_or_stage(self):
        out = os.path.join(self.tmp, "pkg_fail")

        def boom():
            raise OSError("disk full at publish")

        with self.assertRaises(OSError):
            export_review_package(
                self.idx, AST, seed="s", quota_true=1, quota_false=1,
                quota_unknown=1, max_children=10, out_dir=out,
                before_publish=boom)
        self.assertFalse(os.path.lexists(out))
        leftovers = [n for n in os.listdir(self.tmp)
                     if n.startswith(".bitgroup_review_stage_")]
        self.assertEqual(leftovers, [])

    def test_export_failure_midway_leaves_no_package(self):
        out = os.path.join(self.tmp, "pkg_mid")
        real = review_mod._GroupSorter.iter_sorted
        calls = {"n": 0}

        def flaky(self, a, b):
            calls["n"] += 1
            if calls["n"] == 2:
                raise OSError("child column read failed")
            yield from real(self, a, b)

        with mock.patch.object(review_mod._GroupSorter, "iter_sorted", flaky):
            with self.assertRaises(OSError):
                self.export(out, quota_true=5, quota_false=5,
                            quota_unknown=5, max_children=1000)
        self.assertFalse(os.path.lexists(out))
        leftovers = [n for n in os.listdir(self.tmp)
                     if n.startswith(".bitgroup_review_stage_")]
        self.assertEqual(leftovers, [])

    def test_stale_scratch_removed(self):
        out = os.path.join(self.tmp, "pkg_scratch")
        with mock.patch.object(review_mod, "_GroupSorter",
                               side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                self.export(out)
        self.assertFalse(os.path.lexists(out))
        leftovers = [n for n in os.listdir(self.tmp)
                     if n.startswith("bitgroup_review_")]
        self.assertEqual(leftovers, [])

    def test_invalid_quota_rejected(self):
        for kw in ({"quota_true": -1}, {"quota_false": True},
                   {"quota_unknown": 1.5}, {"max_children": -3}):
            with self.assertRaises(ReviewError):
                self.export(os.path.join(self.tmp, "bad"), **kw)
        self.assertFalse(os.path.lexists(os.path.join(self.tmp, "bad")))

    # ---- 只读、可移走原文件 --------------------------------------------

    def test_works_after_moving_originals(self):
        moved = os.path.join(self.tmp, "moved")
        os.makedirs(moved)
        os.rename(self.pp, os.path.join(moved, "p.jsonl"))
        os.rename(self.cp, os.path.join(moved, "c.jsonl"))
        out = os.path.join(self.tmp, "pkg")
        res = self.export(out)
        self.assertEqual(res.manifest["n_parents"], len(PARENT_ROWS))

    # ---- 导出阶段只读中选组 --------------------------------------------

    def test_export_only_reads_selected_groups(self):
        # S 有 2 条子记录且未中选；导出计数只应覆盖中选对象的子记录
        out = os.path.join(self.tmp, "pkg")
        res = self.export(out, quota_true=1, quota_false=0,
                          quota_unknown=1, max_children=1000)
        n_sel_children = res.manifest["n_children"]
        m = res.metrics
        self.assertEqual(m["export_child_ids_read"], n_sel_children)
        self.assertEqual(m["export_child_attr_reads"],
                         n_sel_children * len(CS))
        self.assertEqual(m["export_parent_attr_reads"],
                         res.manifest["n_parents"] * len(PS))
        # 未中选的 S 不在导出组里
        self.assertLess(m["export_groups"], len(PARENT_ROWS))


class ReviewSpoolCase(ReviewCase):
    """暂存与组内排序的边界（块上限很小时仍正确）。"""

    def test_key_spool_multiblock_merge(self):
        rows = [{"entity_id": f"E{i:04d}", "region": "N", "level": i,
                 "flag": True, "note": None} for i in range(300)]
        pp, cp = os.path.join(self.tmp, "kp.jsonl"), os.path.join(
            self.tmp, "kc.jsonl")
        write_jsonl(pp, rows)
        write_jsonl(cp, [])
        idx = os.path.join(self.tmp, "kidx")
        Importer(idx, PS, CS).run(pp, cp)
        spool_dir = tempfile.mkdtemp(dir=self.tmp)
        from bitgroup.review import _KeySpool
        with mock.patch.object(_KeySpool, "_BLOCK_BYTES", 200):
            sp = _KeySpool(spool_dir, "seed")
            for i, r in enumerate(rows):
                sp.add(r["entity_id"], "true", i, 0)
            sp.finish_writes()
            self.assertGreater(sp.blocks_written, 1)
            got = [e for _d, e, _s, _n, _p in sp.iter_ordered()]
            sp.close()
        want = sorted([r["entity_id"] for r in rows],
                      key=lambda e: (priority_digest("seed", e),
                                     e.encode("utf-8")))
        self.assertEqual(got, want)

    def test_group_sorter_multichunk(self):
        rows = [{"child_id": f"c{i:05d}", "entity_id": "A", "temp": i,
                 "source": "X", "ok": True, "tag": None}
                for i in range(250)]
        pp, cp = os.path.join(self.tmp, "gp.jsonl"), os.path.join(
            self.tmp, "gc.jsonl")
        write_jsonl(pp, [{"entity_id": "A", "region": "N", "level": 1,
                          "flag": True, "note": None}])
        write_jsonl(cp, rows)
        idx = os.path.join(self.tmp, "gidx")
        Importer(idx, PS, CS).run(pp, cp)
        scratch = tempfile.mkdtemp(dir=self.tmp)
        from bitgroup.review import _GroupSorter
        with Engine(idx) as eng:
            start, end = eng.ident.child_range(0)
            s = _GroupSorter(eng, scratch, chunk_entries=37)
            got = list(s.iter_sorted(start, end))
        self.assertGreater(s.runs_written, 1)
        self.assertEqual([cid for cid, _ in got],
                         sorted([r["child_id"] for r in rows],
                                key=lambda x: x.encode("utf-8")))
        self.assertEqual(len(got), len(rows))

    def test_iter_child_ids_range(self):
        rows = [{"child_id": f"x{i:04d}", "entity_id": "A", "temp": i,
                 "source": "X", "ok": True, "tag": None}
                for i in range(1200)]
        pp, cp = os.path.join(self.tmp, "ip.jsonl"), os.path.join(
            self.tmp, "ic.jsonl")
        write_jsonl(pp, [{"entity_id": "A", "region": "N", "level": 1,
                          "flag": True, "note": None}])
        write_jsonl(cp, rows)
        idx = os.path.join(self.tmp, "iidx")
        Importer(idx, PS, CS).run(pp, cp)
        with Engine(idx) as eng:
            got = list(eng.ident.iter_child_ids(1010, 1030))
        self.assertEqual(got, [f"x{i:04d}" for i in range(1010, 1030)])


if __name__ == "__main__":
    unittest.main()
