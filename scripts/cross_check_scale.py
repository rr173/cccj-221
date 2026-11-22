"""规模化正确性交叉核对：位图引擎 vs 独立逐行参考解释器。

在可承受全量驻留的中等规模数据上，对一组覆盖稀疏/稠密/三态/嵌套/阈值的
AST，逐状态比较两套实现的计数与成员集合，并校验分面、explain 上下界。
参考解释器刻意不 import 任何产品位图/提升代码。

用法::

    PYTHONPATH=. python scripts/cross_check_scale.py --work-dir ./xc \
        --parents 20000 --children 120000
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bitgroup.engine import Engine  # noqa: E402
from bitgroup.importer import Importer  # noqa: E402
from bitgroup.reference import ReferenceInterpreter  # noqa: E402
from generate_scale import generate  # noqa: E402

PARENT_SCHEMA = {
    "region": "text", "level": "int64", "flag": "bool", "note": "null"
}
CHILD_SCHEMA = {
    "temp": "int64", "source": "text", "ok": "bool", "tag": "null"
}


def c(domain, attr, kind, value):
    return {"op": "cmp", "domain": domain, "attr": attr,
            "op_kind": kind, "value": value}


def build_asts():
    child_and = {"op": "and", "args": [
        c("child", "temp", "lt", 25), c("child", "source", "eq", "X")]}
    child_or = {"op": "or", "args": [
        c("child", "ok", "eq", True), c("child", "temp", "gt", 40)]}
    return [
        c("parent", "region", "eq", "N"),
        c("parent", "level", "gt", 500000),
        c("parent", "flag", "eq", True),
        c("parent", "level", "eq", None),
        {"op": "not", "arg": c("parent", "region", "eq", "N")},
        {"op": "exists", "arg": child_and},
        {"op": "count_where", "arg": child_and, "k": 1},
        {"op": "count_where", "arg": child_and, "k": 3},
        {"op": "count_where", "arg": child_and, "k": 0},
        {"op": "exists", "arg": c("child", "temp", "eq", None)},
        {"op": "not", "arg": {"op": "exists", "arg": child_or}},
        {"op": "and", "args": [
            c("parent", "region", "eq", "N"),
            {"op": "count_where", "arg": child_and, "k": 2}]},
        {"op": "or", "args": [
            {"op": "exists", "arg": child_and},
            c("parent", "flag", "eq", False)]},
        {"op": "not", "arg": {"op": "not", "arg": {
            "op": "exists", "arg": child_or}}},
    ]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", required=True)
    ap.add_argument("--parents", type=int, default=20000)
    ap.add_argument("--children", type=int, default=120000)
    args = ap.parse_args()

    data_dir = os.path.join(args.work_dir, "data")
    idx_dir = os.path.join(args.work_dir, "index")
    parent_path = os.path.join(data_dir, "parent.jsonl")
    child_path = os.path.join(data_dir, "child.jsonl")
    generate(data_dir, args.parents, args.children)
    Importer(idx_dir, PARENT_SCHEMA, CHILD_SCHEMA).run(parent_path, child_path)

    ref = ReferenceInterpreter(parent_path, child_path)
    failures = 0
    with Engine(idx_dir) as eng:
        for ast in build_asts():
            ref_map = ref.evaluate_all(ast)
            for state, want_ids in ref_map.items():
                got = eng.query(ast, state=state, page_size=100000)
                if got.counts[state] != len(want_ids) or \
                        got.page_entity_ids != want_ids:
                    failures += 1
                    print("MISMATCH", state, json.dumps(ast))
                    print("  want", len(want_ids), "got", got.counts[state])
            # 分面交叉核对
            for attr in ("region", "flag"):
                f = eng.facets(ast, attr)
                rf = ref.facets(ast, attr)
                got_vals = {x["value"]: x["count"] for x in f["values"]}
                if got_vals != rf["values"] or f["null_count"] != rf["null_count"]:
                    failures += 1
                    print("FACET MISMATCH", attr, json.dumps(ast))
    if failures:
        print(f"FAILED with {failures} mismatches")
        return 1
    print(f"OK: {len(build_asts())} ASTs x 3 states + facets all match "
          f"reference on {args.parents} parents / {args.children} children")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
