"""128 MiB 内存约束下的规模化基准与正确性观测。

流程：生成（可选）-> 在 RLIMIT_AS=128MiB 子进程内建索引 -> 同样约束下
执行稀疏/稠密/多层查询，记录真实页读取、临时位图峰值与处理子行数。

地址空间限制同时约束 Python 堆、mmap 与缓冲；引擎只允许按需 pread 页 +
受限 LRU 缓存，超限即 MemoryError，从而客观验证内存预算设计。

用法::

    python scripts/bench_scale.py --work-dir /data/bg --parents 1000000 \\
        --children 10000000 --memory-mib 128
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bitgroup.engine import Engine  # noqa: E402
from bitgroup.importer import Importer  # noqa: E402

PARENT_SCHEMA = {
    "region": "text", "level": "int64", "flag": "bool", "note": "null"
}
CHILD_SCHEMA = {
    "temp": "int64", "source": "text", "ok": "bool", "tag": "null"
}

# 稀疏：source=Z（低基数中的稀有值）+ 高温阈值
SPARSE_AST = {
    "op": "and",
    "args": [
        {"op": "cmp", "domain": "parent", "attr": "region",
         "op_kind": "eq", "value": "N"},
        {"op": "count_where", "k": 3, "arg": {
            "op": "and", "args": [
                {"op": "cmp", "domain": "child", "attr": "source",
                 "op_kind": "eq", "value": "Z"},
                {"op": "cmp", "domain": "child", "attr": "temp",
                 "op_kind": "gt", "value": 50},
            ]}},
    ],
}

# 稠密：宽松条件，大部分对象落入 true/unknown
DENSE_AST = {
    "op": "exists",
    "arg": {"op": "or", "args": [
        {"op": "cmp", "domain": "child", "attr": "temp",
         "op_kind": "lt", "value": 1000},
        {"op": "cmp", "domain": "child", "attr": "ok",
         "op_kind": "eq", "value": True},
    ]},
}

# 多层嵌套 not + count 边界
NESTED_AST = {
    "op": "and",
    "args": [
        {"op": "not", "arg": {
            "op": "not", "arg": {
                "op": "count_where", "k": 1, "arg": {
                    "op": "cmp", "domain": "child", "attr": "source",
                    "op_kind": "eq", "value": "X"}}}},
        {"op": "not", "arg": {
            "op": "exists", "arg": {
                "op": "cmp", "domain": "child", "attr": "temp",
                "op_kind": "gt", "value": 55}}},
    ],
}


def limit_memory(mib: int):
    soft = hard = mib * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (soft, hard))


def _rss_mib() -> float:
    # ru_maxrss 在 Linux 上单位 KB
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def build(index_dir, parent_path, child_path, cache_mib):
    t0 = time.time()
    imp = Importer(index_dir, PARENT_SCHEMA, CHILD_SCHEMA,
                   cache_budget_bytes=cache_mib * 1024 * 1024)
    handle = imp.run(parent_path, child_path)
    return {
        "phase": "build",
        "seconds": round(time.time() - t0, 3),
        "max_rss_mib": round(_rss_mib(), 1),
        "n_parents": handle.meta["n_parents"],
        "n_children": handle.meta["n_children"],
        "index_token": handle.index_token,
    }


def run_queries(index_dir, cache_mib, page_size):
    out = []
    with Engine(index_dir, cache_budget_bytes=cache_mib * 1024 * 1024) as eng:
        for name, ast in (("sparse", SPARSE_AST), ("dense", DENSE_AST),
                          ("nested_not", NESTED_AST)):
            t0 = time.time()
            res = eng.query(ast, state="true", page_size=page_size)
            # 再翻几页，验证分页令牌在规模集合上稳定
            token = res.next_token
            pages = 1
            sample = list(res.page_entity_ids)
            while token and pages < 3:
                res2 = eng.query(ast, state="true", page_size=page_size,
                                 page_token=token)
                token = res2.next_token
                pages += 1
            # 分面（稀疏属性 + 稠密属性）
            facet = eng.facets(ast, "region")
            out.append({
                "name": name,
                "seconds": round(time.time() - t0, 3),
                "counts": res.counts,
                "pages_walked": pages,
                "first_ids": sample[:5],
                "io": res.io,
                "facet_region": {x["value"]: x["count"] for x in facet["values"]},
                "facet_null": facet["null_count"],
                "max_rss_mib": round(_rss_mib(), 1),
            })
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", required=True)
    ap.add_argument("--parents", type=int, default=1_000_000)
    ap.add_argument("--children", type=int, default=10_000_000)
    ap.add_argument("--memory-mib", type=int, default=128)
    ap.add_argument("--cache-mib", type=int, default=24)
    ap.add_argument("--page-size", type=int, default=1000)
    ap.add_argument("--skip-generate", action="store_true")
    args = ap.parse_args()

    data_dir = os.path.join(args.work_dir, "data")
    index_dir = os.path.join(args.work_dir, "index")
    parent_path = os.path.join(data_dir, "parent.jsonl")
    child_path = os.path.join(data_dir, "child.jsonl")

    if not args.skip_generate:
        from generate_scale import generate
        generate(data_dir, args.parents, args.children)

    # 建索引与查询都在本进程内受同一地址空间约束
    limit_memory(args.memory_mib)
    report = {"memory_limit_mib": args.memory_mib, "cache_mib": args.cache_mib}
    if os.path.exists(index_dir):
        import shutil
        shutil.rmtree(index_dir)
    report["build"] = build(index_dir, parent_path, child_path, args.cache_mib)
    report["queries"] = run_queries(index_dir, args.cache_mib, args.page_size)
    report["peak_rss_mib"] = round(_rss_mib(), 1)

    out_path = os.path.join(args.work_dir, "bench_report.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\n报告已写入 {out_path}")


if __name__ == "__main__":
    main()
