"""端到端测试：导入 -> 位图引擎查询，与独立解释器逐对象核对。

直接用标准库 unittest 运行：
    PYTHONPATH=/workspace python3 -m unittest discover -s tests -v
"""

import json
import os
import shutil
import tempfile
import unittest

from bitgroup.engine import Engine
from bitgroup.errors import InputDataError, QueryError, TokenError
from bitgroup.importer import Importer
from bitgroup.reference import ReferenceInterpreter, T as RT, F as RF, U as RU

PARENT_SCHEMA = {"region": "text", "level": "int64", "flag": "bool", "note": "null"}
CHILD_SCHEMA = {"temp": "int64", "source": "text", "ok": "bool", "tag": "null"}

PARENT_ROWS = [
    {"entity_id": "P", "region": "N", "level": None, "flag": True, "note": None},
    {"entity_id": "Q", "region": "N", "level": 2, "flag": False, "note": None},
    {"entity_id": "R", "region": "S", "level": 3, "flag": None, "note": None},
    {"entity_id": "S", "region": "N", "level": 1, "flag": True, "note": None},
]
CHILD_ROWS = [
    {"child_id": "p1", "entity_id": "P", "temp": 20, "source": "Y", "ok": True, "tag": None},
    {"child_id": "p2", "entity_id": "P", "temp": None, "source": "X", "ok": False, "tag": None},
    {"child_id": "q1", "entity_id": "Q", "temp": 20, "source": "X", "ok": True, "tag": None},
    {"child_id": "q2", "entity_id": "Q", "temp": 21, "source": "X", "ok": None, "tag": None},
    # S: 多个 unknown、重复业务值但 child_id 不同
    {"child_id": "s1", "entity_id": "S", "temp": None, "source": "X", "ok": True, "tag": None},
    {"child_id": "s2", "entity_id": "S", "temp": None, "source": "X", "ok": True, "tag": None},
    {"child_id": "s3", "entity_id": "S", "temp": 30, "source": "Z", "ok": False, "tag": None},
]

CHILD_PRED = {
    "op": "and",
    "args": [
        {"op": "cmp", "domain": "child", "attr": "temp", "op_kind": "lt", "value": 25},
        {"op": "cmp", "domain": "child", "attr": "source", "op_kind": "eq", "value": "X"},
    ],
}


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


class EngineCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bgtest_")
        self.parent_path = os.path.join(self.tmp, "parent.jsonl")
        self.child_path = os.path.join(self.tmp, "child.jsonl")
        write_jsonl(self.parent_path, PARENT_ROWS)
        write_jsonl(self.child_path, CHILD_ROWS)
        self.idx = os.path.join(self.tmp, "idx")
        Importer(self.idx, PARENT_SCHEMA, CHILD_SCHEMA).run(
            self.parent_path, self.child_path
        )
        self.ref = ReferenceInterpreter(self.parent_path, self.child_path)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def engine(self, budget=4 * 1024 * 1024):
        return Engine(self.idx, cache_budget_bytes=budget)

    # ---- 手算示例 ------------------------------------------------------

    def test_hand_example_exists_and_counts(self):
        exists_ast = {"op": "exists", "arg": CHILD_PRED}
        count2 = {"op": "count_where", "arg": CHILD_PRED, "k": 2}
        count0 = {"op": "count_where", "arg": CHILD_PRED, "k": 0}
        with self.engine() as eng:
            r = eng.query(exists_ast)
            # Q=true；P、S 无 true 但有 unknown -> unknown；R 无子女 -> false
            self.assertEqual(r.counts, {"true": 1, "unknown": 2, "false": 1})
            self.assertEqual(r.page_entity_ids, ["Q"])
            r2 = eng.query(count2)
            # Q(2/2) true；S(0/2) 满足 lb<2<=ub -> unknown；P(0/1)、R(0/0) 上界<2 -> false
            self.assertEqual(r2.counts, {"true": 1, "unknown": 1, "false": 2})
            self.assertEqual(r2.page_entity_ids, ["Q"])
            r0 = eng.query(count0)
            self.assertEqual(r0.counts["true"], 4)  # count>=0 恒 true

    def test_not_exists_full_domain(self):
        not_exists = {"op": "not", "arg": {"op": "exists", "arg": CHILD_PRED}}
        with self.engine() as eng:
            # 无子女的 R 在补集内 -> true；P、S unknown 保持；Q false
            ids = {s: eng.query(not_exists, state=s).page_entity_ids
                   for s in ("true", "unknown", "false")}
            self.assertEqual(ids["true"], ["R"])
            self.assertEqual(ids["unknown"], ["P", "S"])
            self.assertEqual(ids["false"], ["Q"])

    def test_wrong_intersection_anti_pattern(self):
        """错误做法：把'有合格温度'和'有X来源'两父集合相交，会误判 P。
        产品引擎必须用同一条子行断言，P 只能是 unknown。"""
        wrong = {
            "op": "and",
            "args": [
                {"op": "exists", "arg": {
                    "op": "cmp", "domain": "child", "attr": "temp",
                    "op_kind": "lt", "value": 25}},
                {"op": "exists", "arg": {
                    "op": "cmp", "domain": "child", "attr": "source",
                    "op_kind": "eq", "value": "X"}},
            ],
        }
        correct = {"op": "exists", "arg": CHILD_PRED}
        with self.engine() as eng:
            rw = eng.query(wrong)
            rc = eng.query(correct)
            # 错误 AST 确实会把 P 判 true —— 用它证明差异真实存在
            self.assertIn("P", rw.page_entity_ids)
            # 正确的同子行断言：P 是 unknown 而非 true（S 同样为 unknown）
            self.assertNotIn("P", rc.page_entity_ids)
            self.assertEqual(
                eng.query(correct, state="unknown").page_entity_ids, ["P", "S"]
            )

    # ---- 与参考解释器全量交叉核对 --------------------------------------

    def test_cross_check_many_asts(self):
        asts = self._make_asts()
        with self.engine() as eng:
            for ast in asts:
                ref_map = self.ref.evaluate_all(ast)
                for state, ref_ids in ref_map.items():
                    got = eng.query(ast, state=state, page_size=50).page_entity_ids
                    self.assertEqual(
                        got, ref_ids,
                        msg=f"state={state} ast={json.dumps(ast)}",
                    )

    def _make_asts(self):
        c = lambda d, a, k, v: {"op": "cmp", "domain": d, "attr": a,
                                "op_kind": k, "value": v}
        pred_x = {"op": "and", "args": [
            c("child", "temp", "lt", 25), c("child", "source", "eq", "X")]}
        pred_y = {"op": "or", "args": [
            c("child", "ok", "eq", True), c("child", "temp", "ge", 21)]}
        null_temp = c("child", "temp", "eq", None)
        return [
            c("parent", "region", "eq", "N"),
            c("parent", "region", "ne", "N"),
            c("parent", "level", "gt", 1),
            c("parent", "level", "le", 2),
            c("parent", "level", "eq", None),
            c("parent", "flag", "eq", True),
            c("parent", "flag", "ne", True),
            c("parent", "flag", "eq", None),
            {"op": "not", "arg": c("parent", "region", "eq", "N")},
            {"op": "and", "args": [
                c("parent", "region", "eq", "N"),
                {"op": "or", "args": [
                    c("parent", "level", "ge", 2),
                    c("parent", "flag", "eq", True)]}]},
            {"op": "exists", "arg": pred_x},
            {"op": "count_where", "arg": pred_x, "k": 1},
            {"op": "count_where", "arg": pred_x, "k": 2},
            {"op": "count_where", "arg": pred_x, "k": 3},
            {"op": "exists", "arg": null_temp},
            {"op": "not", "arg": {"op": "exists", "arg": pred_y}},
            {"op": "and", "args": [
                c("parent", "region", "eq", "N"),
                {"op": "count_where", "arg": pred_x, "k": 1}]},
            {"op": "not", "arg": {"op": "not", "arg":
                {"op": "exists", "arg": pred_x}}},
        ]

    # ---- 分页与令牌 ----------------------------------------------------

    def test_paging_sizes_and_token_binding(self):
        ast = {"op": "count_where", "arg": CHILD_PRED, "k": 0}  # 全 true
        with self.engine() as eng:
            all_ids = []
            token = None
            for _ in range(10):
                r = eng.query(ast, page_size=2, page_token=token)
                all_ids.extend(r.page_entity_ids)
                token = r.next_token
                if token is None:
                    break
            self.assertEqual(all_ids, [p["entity_id"] for p in PARENT_ROWS])
            # 不同 page_size 的旧令牌必须拒绝
            r1 = eng.query(ast, page_size=2)
            self.assertIsNotNone(r1.next_token)
            with self.assertRaises(TokenError):
                eng.query(ast, page_size=3, page_token=r1.next_token)

    def test_token_rejected_for_other_query(self):
        ast1 = {"op": "cmp", "domain": "parent", "attr": "region",
                "op_kind": "eq", "value": "N"}
        ast2 = {"op": "cmp", "domain": "parent", "attr": "region",
                "op_kind": "eq", "value": "S"}
        with self.engine() as eng:
            tok = eng.query(ast1, page_size=1).next_token
            self.assertIsNotNone(tok)
            with self.assertRaises(TokenError):
                eng.query(ast2, page_size=1, page_token=tok)

    def test_token_rejected_for_other_index(self):
        idx2 = os.path.join(self.tmp, "idx2")
        Importer(idx2, PARENT_SCHEMA, CHILD_SCHEMA).run(
            self.parent_path, self.child_path)
        ast = {"op": "cmp", "domain": "parent", "attr": "region",
               "op_kind": "eq", "value": "N"}
        with Engine(self.idx) as e1, Engine(idx2) as e2:
            tok = e1.query(ast, page_size=1).next_token
            with self.assertRaises(TokenError):
                e2.query(ast, page_size=1, page_token=tok)

    # ---- 分面 ----------------------------------------------------------

    def test_facets_match_reference(self):
        ast = {"op": "exists", "arg": CHILD_PRED}
        with self.engine() as eng:
            for attr in ("region", "level", "flag"):
                got = eng.facets(ast, attr)
                want = self.ref.facets(ast, attr)
                got_values = {x["value"]: x["count"] for x in got["values"]}
                self.assertEqual(got_values, want["values"], attr)
                self.assertEqual(got["null_count"], want["null_count"], attr)

    # ---- explain -------------------------------------------------------

    def test_explain_matches_reference(self):
        ast = {"op": "and", "args": [
            {"op": "cmp", "domain": "parent", "attr": "region",
             "op_kind": "eq", "value": "N"},
            {"op": "exists", "arg": CHILD_PRED}]}
        with self.engine() as eng:
            for eid in ("P", "Q", "R", "S"):
                got = eng.explain_entity(ast, eid)["root"]
                want = self.ref.explain(ast, eid)
                self.assertEqual(got["state"], want["state"], eid)
                lift = got["args"][1]
                wlift = want["args"][1]
                self.assertEqual(lift["lower_bound"], wlift["lower_bound"], eid)
                self.assertEqual(lift["upper_bound"], wlift["upper_bound"], eid)
                self.assertEqual(lift["no_children"], wlift["no_children"], eid)

    def test_explain_null_vs_no_children_distinct(self):
        ast = {"op": "exists", "arg": CHILD_PRED}
        with self.engine() as eng:
            p = eng.explain_entity(ast, "P")["root"]
            r = eng.explain_entity(ast, "R")["root"]
            self.assertFalse(p["no_children"])
            self.assertTrue(r["no_children"])
            # P 的 unknown 来自 null temp，有见证 child_id
            self.assertEqual(p["witnesses"]["unknown"], "p2")
            self.assertIsNone(r["witnesses"]["unknown"])

    # ---- 输入校验 ------------------------------------------------------

    def test_dangling_entity_rejected(self):
        bad = os.path.join(self.tmp, "bad_child.jsonl")
        write_jsonl(bad, [{"child_id": "x", "entity_id": "GHOST",
                           "temp": 1, "source": "X", "ok": True, "tag": None}])
        with self.assertRaises(InputDataError):
            Importer(os.path.join(self.tmp, "i"), PARENT_SCHEMA,
                     CHILD_SCHEMA).run(self.parent_path, bad)

    def test_duplicate_child_id_rejected(self):
        bad = os.path.join(self.tmp, "dup_child.jsonl")
        rows = CHILD_ROWS + [{"child_id": "p1", "entity_id": "P",
                              "temp": 1, "source": "X", "ok": True, "tag": None}]
        write_jsonl(bad, rows)
        with self.assertRaises(InputDataError):
            Importer(os.path.join(self.tmp, "i"), PARENT_SCHEMA,
                     CHILD_SCHEMA).run(self.parent_path, bad)

    def test_duplicate_parent_id_rejected(self):
        bad = os.path.join(self.tmp, "dup_parent.jsonl")
        write_jsonl(bad, PARENT_ROWS + [PARENT_ROWS[0]])
        with self.assertRaises(InputDataError):
            Importer(os.path.join(self.tmp, "i"), PARENT_SCHEMA,
                     CHILD_SCHEMA).run(bad, self.child_path)

    def test_bool_not_equal_to_one_rejected_on_import(self):
        bad = os.path.join(self.tmp, "bad_parent.jsonl")
        rows = [dict(r, flag=1) for r in PARENT_ROWS]
        write_jsonl(bad, rows)
        with self.assertRaises(InputDataError):
            Importer(os.path.join(self.tmp, "i"), PARENT_SCHEMA,
                     CHILD_SCHEMA).run(bad, self.child_path)

    def test_bool_not_equal_to_one_query_type_error(self):
        # bool 不等于数字 1：跨类型比较在 AST 校验期直接拒绝
        ast = {"op": "cmp", "domain": "parent", "attr": "flag",
               "op_kind": "eq", "value": 1}
        with self.engine() as eng:
            with self.assertRaises(QueryError):
                eng.query(ast)

    def test_empty_string_is_value_not_null(self):
        rows = [dict(r, region="") for r in PARENT_ROWS]
        p = os.path.join(self.tmp, "empty.jsonl")
        write_jsonl(p, rows)
        idx = os.path.join(self.tmp, "i")
        Importer(idx, PARENT_SCHEMA, CHILD_SCHEMA).run(p, self.child_path)
        ast = {"op": "cmp", "domain": "parent", "attr": "region",
               "op_kind": "eq", "value": ""}
        with Engine(idx) as eng:
            r = eng.query(ast)
            self.assertEqual(r.counts["true"], len(rows))
            self.assertEqual(r.counts["unknown"], 0)

    # ---- 可重开与原文件移走 --------------------------------------------

    def test_reopen_and_move_originals(self):
        moved = os.path.join(self.tmp, "moved")
        os.makedirs(moved)
        os.rename(self.parent_path, os.path.join(moved, "p.jsonl"))
        os.rename(self.child_path, os.path.join(moved, "c.jsonl"))
        with self.engine() as eng:
            r = eng.query({"op": "exists", "arg": CHILD_PRED})
            self.assertEqual(r.counts["true"], 1)
            # 身份可逆
            self.assertEqual(eng.ident.entity_id_at(0), "P")
            self.assertEqual(eng.ident.pid_of("Q"), 1)
            self.assertEqual(eng.ident.child_id_at(0), "p1")
            self.assertEqual(eng.ident.cid_of("q2"), 3)

    # ---- 可观测性 ------------------------------------------------------

    def test_sparse_query_does_not_touch_unrelated_postings(self):
        # 稀疏等值条件：只应打开 source 属性，不打开 temp/ok
        ast = {"op": "cmp", "domain": "child", "attr": "source",
               "op_kind": "eq", "value": "X"}
        with self.engine() as eng:
            before = dict(eng.monitor.snapshot())
            eng.query(ast)
            after = eng.monitor.snapshot()
            self.assertGreater(after["posting_files_opened"] -
                               before["posting_files_opened"], 0)
        # 重新打开，确认 temp  posting 未被该查询打开
        with self.engine() as eng:
            eng.query(ast)
            opened = [k for k in eng.registry._cache.keys()]
            self.assertIn(("child", "source"), opened)
            self.assertNotIn(("child", "temp"), opened)

    def test_child_rows_processed_counter(self):
        ast = {"op": "exists", "arg": CHILD_PRED}
        with self.engine() as eng:
            r = eng.query(ast)
            self.assertEqual(r.io["child_rows_processed"], len(CHILD_ROWS))


if __name__ == "__main__":
    unittest.main()
