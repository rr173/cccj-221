"""int64 范围与符号、外部排序 posting 的回归测试。

覆盖：负数/正数混合、窄域高倾斜、范围比较各 op、null 与取值互斥、
重复值的 posting 成员完整性。
"""

import json
import os
import shutil
import tempfile
import unittest

from bitgroup.engine import Engine
from bitgroup.importer import Importer

PS = {"g": "text"}
CS = {"v": "int64", "w": "int64"}


def w(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


class IntRangeCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bgint_")
        pp = os.path.join(self.tmp, "p.jsonl")
        cp = os.path.join(self.tmp, "c.jsonl")
        # 父对象各带一个子行，v 覆盖负数/零/正数/null；w 窄域高倾斜
        vals = [-100, -20, -1, 0, 1, 20, 100, None, -20, 20]
        parents = [{"entity_id": f"e{i}", "g": "N" if i % 2 else "S"}
                   for i in range(len(vals))]
        children = [
            {"child_id": f"k{i}", "entity_id": f"e{i}",
             "v": vals[i], "w": (i % 5) - 2}  # -2..2 窄域
            for i in range(len(vals))
        ]
        w(pp, parents)
        w(cp, children)
        self.idx = os.path.join(self.tmp, "idx")
        Importer(self.idx, PS, CS).run(pp, cp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def state_ids(self, eng, ast, state):
        return eng.query(ast, state=state, page_size=1000).page_entity_ids

    def test_range_comparisons(self):
        with Engine(self.idx) as eng:
            def ast(op, val):
                return {"op": "exists", "arg": {
                    "op": "cmp", "domain": "child", "attr": "v",
                    "op_kind": op, "value": val}}
            # v < 0：-100,-20,-1,-20 -> e0,e1,e2,e8
            self.assertEqual(
                sorted(self.state_ids(eng, ast("lt", 0), "true")),
                ["e0", "e1", "e2", "e8"])
            # v <= -20：-100,-20,-20
            self.assertEqual(
                sorted(self.state_ids(eng, ast("le", -20), "true")),
                ["e0", "e1", "e8"])
            # v > 0：1,20,100,20
            self.assertEqual(
                sorted(self.state_ids(eng, ast("gt", 0), "true")),
                ["e4", "e5", "e6", "e9"])
            # v >= 100：仅 100
            self.assertEqual(self.state_ids(eng, ast("ge", 100), "true"),
                             ["e6"])
            # v == 20：重复值两个对象
            self.assertEqual(
                sorted(self.state_ids(eng, ast("eq", 20), "true")),
                ["e5", "e9"])
            # v != 20：所有非 null 且不等于 20
            ne = set(self.state_ids(eng, ast("ne", 20), "true"))
            self.assertNotIn("e5", ne)
            self.assertNotIn("e9", ne)
            self.assertNotIn("e7", ne)  # null -> unknown，不在 true

    def test_null_is_unknown_and_exclusive(self):
        with Engine(self.idx) as eng:
            ast = {"op": "exists", "arg": {
                "op": "cmp", "domain": "child", "attr": "v",
                "op_kind": "eq", "value": 0}}
            # e7 的 v=null -> exists unknown
            self.assertEqual(self.state_ids(eng, ast, "unknown"), ["e7"])
            r = eng.query(ast)
            self.assertEqual(sum(r.counts.values()), 10)

    def test_narrow_skewed_column(self):
        # w 仅取值 -2..2，验证高倾斜列的 posting 成员与计数
        with Engine(self.idx) as eng:
            for val in (-2, -1, 0, 1, 2):
                ast = {"op": "exists", "arg": {
                    "op": "cmp", "domain": "child", "attr": "w",
                    "op_kind": "eq", "value": val}}
                ids = self.state_ids(eng, ast, "true")
                expect = [f"e{i}" for i in range(10) if (i % 5) - 2 == val]
                self.assertEqual(sorted(ids), sorted(expect), val)


if __name__ == "__main__":
    unittest.main()
