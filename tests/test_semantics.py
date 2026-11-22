"""补充语义测试：多 unknown、重复业务值、主属性 null、k 边界、极大子组、
嵌套 not、分页大小与跨索引令牌。全部与独立参考解释器交叉核对。
"""

import json
import os
import shutil
import tempfile
import unittest

from bitgroup.engine import Engine
from bitgroup.errors import TokenError
from bitgroup.importer import Importer
from bitgroup.reference import ReferenceInterpreter

PS = {"zone": "text", "score": "int64", "on": "bool"}
CS = {"v": "int64", "kind": "text", "ok": "bool"}


def w(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


class SemanticsCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bgsem_")
        self.pp = os.path.join(self.tmp, "p.jsonl")
        self.cp = os.path.join(self.tmp, "c.jsonl")
        # 父对象：A 极大子组；B 多 unknown；C 重复业务值不同 child_id；
        # D 主属性含 null；E 无子行
        parents = [
            {"entity_id": "A", "zone": "N", "score": 10, "on": True},
            {"entity_id": "B", "zone": "N", "score": None, "on": False},
            {"entity_id": "C", "zone": "S", "score": 5, "on": None},
            {"entity_id": "D", "zone": None, "score": 7, "on": True},
            {"entity_id": "E", "zone": "N", "score": 9, "on": True},
        ]
        children = []
        # A: 200 个子行，制造极大子组；其中 60 true、40 unknown、其余 false
        for i in range(200):
            if i < 60:
                children.append({"child_id": f"a{i}", "entity_id": "A",
                                 "v": 1, "kind": "X", "ok": True})
            elif i < 100:
                children.append({"child_id": f"a{i}", "entity_id": "A",
                                 "v": None, "kind": "X", "ok": True})
            else:
                children.append({"child_id": f"a{i}", "entity_id": "A",
                                 "v": 9, "kind": "Y", "ok": False})
        # B: 三个 unknown 子行（v 全 null）
        for i in range(3):
            children.append({"child_id": f"b{i}", "entity_id": "B",
                             "v": None, "kind": "X", "ok": True})
        # C: 重复业务值（v=1, kind=X）但 child_id 不同
        for i in range(4):
            children.append({"child_id": f"c{i}", "entity_id": "C",
                             "v": 1, "kind": "X", "ok": True})
        w(self.pp, parents)
        w(self.cp, children)
        self.idx = os.path.join(self.tmp, "idx")
        Importer(self.idx, PS, CS).run(self.pp, self.cp)
        self.ref = ReferenceInterpreter(self.pp, self.cp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def cmp(self, attr, kind, val, domain="child"):
        return {"op": "cmp", "domain": domain, "attr": attr,
                "op_kind": kind, "value": val}

    def child_pred(self):
        return {"op": "and", "args": [
            self.cmp("v", "eq", 1), self.cmp("kind", "eq", "X")]}

    def assert_matches_reference(self, eng, ast):
        ref_map = self.ref.evaluate_all(ast)
        for state, want in ref_map.items():
            got = eng.query(ast, state=state, page_size=123).page_entity_ids
            self.assertEqual(got, want, f"{state} {json.dumps(ast)}")

    def test_count_k_boundaries_on_huge_group(self):
        # A: true=60, unknown=40 -> 下界 60，上界 100
        with Engine(self.idx) as eng:
            for k in (0, 1, 59, 60):
                ast = {"op": "count_where", "arg": self.child_pred(), "k": k}
                self.assert_matches_reference(eng, ast)
                ex = eng.explain_entity(ast, "A")["root"]
                self.assertEqual((ex["lower_bound"], ex["upper_bound"]), (60, 100))
                self.assertEqual(ex["state"], "true")
            # k=61..100 -> unknown；k=101 -> false
            for k, expect in ((61, "unknown"), (100, "unknown"), (101, "false")):
                ast = {"op": "count_where", "arg": self.child_pred(), "k": k}
                self.assertEqual(
                    eng.explain_entity(ast, "A")["root"]["state"], expect)
                self.assert_matches_reference(eng, ast)

    def test_multiple_unknowns(self):
        # B 三个 unknown 子行：exists=unknown，count>=1 unknown，count>=4 false
        with Engine(self.idx) as eng:
            ex = eng.explain_entity(
                {"op": "exists", "arg": self.child_pred()}, "B")["root"]
            self.assertEqual(ex["state"], "unknown")
            self.assertEqual((ex["lower_bound"], ex["upper_bound"]), (0, 3))
            self.assertIsNotNone(ex["witnesses"]["unknown"])
            self.assertIsNone(ex["witnesses"]["true"])

    def test_duplicate_business_values_distinct_child_ids(self):
        with Engine(self.idx) as eng:
            ast = {"op": "count_where", "arg": self.child_pred(), "k": 4}
            ex = eng.explain_entity(ast, "C")["root"]
            # 4 个不同 child_id 同业务值，全部 true -> 4/4
            self.assertEqual(ex["state"], "true")
            self.assertEqual(ex["lower_bound"], 4)
            cids = [c["child_id"] for c in ex["children"]]
            self.assertEqual(len(set(cids)), 4)

    def test_parent_null_attribute(self):
        with Engine(self.idx) as eng:
            ast = self.cmp("score", "gt", 5, domain="parent")
            # B 的 score 为 null -> unknown；D 的 score=7 是 true
            self.assertEqual(
                eng.query(ast, state="unknown").page_entity_ids, ["B"])
            self.assert_matches_reference(eng, ast)
            # 对 zone（D 为 null）查询
            ast_zone = self.cmp("zone", "eq", "N", domain="parent")
            self.assertEqual(
                eng.query(ast_zone, state="unknown").page_entity_ids, ["D"])
            self.assert_matches_reference(eng, ast_zone)
            # null 字面量：整个域 unknown
            ast_null = self.cmp("score", "eq", None, domain="parent")
            r = eng.query(ast_null)
            self.assertEqual(r.counts["unknown"], 5)
            self.assertEqual(r.counts["true"], 0)

    def test_nested_not_full_domain(self):
        with Engine(self.idx) as eng:
            ast = {"op": "not", "arg": {
                "op": "not", "arg": {
                    "op": "exists", "arg": self.child_pred()}}}
            self.assert_matches_reference(eng, ast)
            # 双重 not 后 E（无子行）仍为 false
            self.assertEqual(
                eng.explain_entity(ast, "E")["root"]["state"], "false")
            # not exists: E=true（补集覆盖无子行对象）
            not_exists = {"op": "not", "arg": {
                "op": "exists", "arg": self.child_pred()}}
            self.assertIn(
                "E", eng.query(not_exists, state="true").page_entity_ids)

    def test_various_page_sizes(self):
        ast = {"op": "count_where", "arg": self.child_pred(), "k": 0}
        with Engine(self.idx) as eng:
            total = eng.query(ast).counts["true"]
            for size in (1, 2, 3, 5, 100):
                seen = []
                token = None
                pages = 0
                while True:
                    r = eng.query(ast, page_size=size, page_token=token)
                    seen.extend(r.page_entity_ids)
                    pages += 1
                    token = r.next_token
                    if token is None:
                        break
                self.assertEqual(len(seen), total)
                self.assertEqual(len(set(seen)), len(seen))
                self.assertEqual(pages, -(-total // size))

    def test_token_bound_to_index_and_query(self):
        idx2 = os.path.join(self.tmp, "idx2")
        Importer(idx2, PS, CS).run(self.pp, self.cp)
        ast1 = self.cmp("zone", "eq", "N", domain="parent")
        ast2 = self.cmp("zone", "eq", "S", domain="parent")
        with Engine(self.idx) as e1, Engine(idx2) as e2:
            tok = e1.query(ast1, page_size=1).next_token
            self.assertIsNotNone(tok)
            # 换查询拒绝
            with self.assertRaises(TokenError):
                e1.query(ast2, page_size=1, page_token=tok)
            # 换索引拒绝
            with self.assertRaises(TokenError):
                e2.query(ast1, page_size=1, page_token=tok)
            # 换状态拒绝
            with self.assertRaises(TokenError):
                e1.query(ast1, state="false", page_size=1, page_token=tok)

    def test_explain_null_vs_no_children_distinct(self):
        with Engine(self.idx) as eng:
            ast = {"op": "exists", "arg": self.child_pred()}
            b = eng.explain_entity(ast, "B")["root"]
            e = eng.explain_entity(ast, "E")["root"]
            self.assertFalse(b["no_children"])
            self.assertTrue(e["no_children"])
            self.assertEqual(b["upper_bound"], 3)
            self.assertEqual(e["lower_bound"], 0)
            self.assertEqual(e["upper_bound"], 0)


if __name__ == "__main__":
    unittest.main()
