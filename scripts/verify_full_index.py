"""全量索引就绪后的功能验收（不依赖原始 JSONL，验证移走原文件仍可查询）。

用法::

    PYTHONPATH=. python scripts/verify_full_index.py --index ./scale_full/index
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bitgroup.engine import Engine  # noqa: E402
from bitgroup.errors import TokenError  # noqa: E402

CMP = lambda d, a, k, v: {"op": "cmp", "domain": d, "attr": a,
                          "op_kind": k, "value": v}

SPARSE = {"op": "and", "args": [
    CMP("parent", "region", "eq", "N"),
    {"op": "count_where", "k": 3, "arg": {"op": "and", "args": [
        CMP("child", "source", "eq", "Z"),
        CMP("child", "temp", "gt", 50)]}}]}

DENSE = {"op": "exists", "arg": {"op": "or", "args": [
    CMP("child", "temp", "lt", 1000),
    CMP("child", "ok", "eq", True)]}}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", required=True)
    ap.add_argument("--page-size", type=int, default=1000)
    args = ap.parse_args()

    report = {"index": args.index, "checks": {}}
    with Engine(args.index, cache_budget_bytes=16 * 1024 * 1024) as eng:
        report["n_parents"] = eng.n_parents
        report["n_children"] = eng.n_children
        report["index_token"] = eng.index_token

        # 1) 三态计数守恒
        r = eng.query(SPARSE, page_size=args.page_size)
        total = sum(r.counts.values())
        report["checks"]["counts_sum_equals_n_parents"] = (total == eng.n_parents)
        report["sparse_counts"] = r.counts

        # 2) 分页遍历 true 集合：只翻前若干页验证令牌衔接、无重复/遗漏
        seen, token, pages = [], None, 0
        max_pages = 5
        while True:
            q = eng.query(SPARSE, page_size=args.page_size, page_token=token)
            seen.extend(q.page_entity_ids)
            pages += 1
            token = q.next_token
            if token is None or pages >= max_pages:
                break
        report["checks"]["pagination_token_chain_works"] = (
            len(seen) == len(set(seen))
            and (token is None or len(seen) == max_pages * args.page_size))
        report["sparse_pages_walked"] = pages

        # 3) 令牌与查询/索引绑定：稠密查询翻 3 页拿到真实令牌，换查询必须拒绝
        dq = eng.query(DENSE, page_size=args.page_size)
        dense_first = list(dq.page_entity_ids)
        token, dpages, dense_seen = dq.next_token, 1, list(dq.page_entity_ids)
        while token is not None and dpages < 3:
            q2 = eng.query(DENSE, page_size=args.page_size, page_token=token)
            dense_seen.extend(q2.page_entity_ids)
            dpages += 1
            token = q2.next_token
        report["dense_pagination_sample"] = {
            "pages": dpages,
            "no_dup": len(dense_seen) == len(set(dense_seen)),
            "total_true": dq.counts["true"]}
        try:
            eng.query(SPARSE, page_size=args.page_size,
                      page_token=dq.next_token)
            report["checks"]["token_rejects_other_query"] = False
        except TokenError:
            report["checks"]["token_rejects_other_query"] = True

        # 4) 身份可逆（抽样首尾与随机中段）
        sample_pids = [0, 1, eng.n_parents // 2, eng.n_parents - 1]
        rev = []
        for pid in sample_pids:
            eid = eng.ident.entity_id_at(pid)
            rev.append(eng.ident.pid_of(eid) == pid)
        report["checks"]["parent_identity_reversible"] = all(rev)
        if eng.n_children:
            cids = [0, eng.n_children // 2, eng.n_children - 1]
            crev = all(eng.ident.cid_of(eng.ident.child_id_at(c)) == c
                       for c in cids)
            report["checks"]["child_identity_reversible"] = crev

        # 5) 分面：null 单列且对象不重复（合计不超过 true 集合）
        f = eng.facets(SPARSE, "region")
        facet_sum = sum(x["count"] for x in f["values"]) + f["null_count"]
        report["checks"]["facet_within_true_set"] = facet_sum <= r.counts["true"]
        report["facet_region"] = {x["value"]: x["count"] for x in f["values"]}
        report["facet_null"] = f["null_count"]

        # 6) explain 一个 true 对象，递归定位 lift 节点校验上下界与见证
        def find_lift(node):
            if node.get("op") in ("exists", "count_where"):
                return node
            for key in ("arg", "args"):
                child = node.get(key)
                if isinstance(child, dict):
                    found = find_lift(child)
                    if found:
                        return found
                elif isinstance(child, list):
                    for c in child:
                        found = find_lift(c)
                        if found:
                            return found
            return None

        if seen:
            ex = eng.explain_entity(SPARSE, seen[0])["root"]
            lift = find_lift(ex)
            report["checks"]["explain_has_bounds_and_state"] = (
                lift is not None
                and "lower_bound" in lift and "upper_bound" in lift
                and lift["state"] in ("true", "false", "unknown"))

        # 7) 稀疏 vs 稠密条件实际触碰的位图页。用各自独立的新引擎会话测量，
        #    避免同一进程内结果缓存/LRU 复用把读取量“摊薄”为 0。
        with Engine(args.index, cache_budget_bytes=16 * 1024 * 1024) as es:
            s_before = es.monitor.snapshot()
            es.query(SPARSE)
            s_delta = es.monitor.delta(s_before)
        with Engine(args.index, cache_budget_bytes=16 * 1024 * 1024) as ed:
            d_before = ed.monitor.snapshot()
            ed.query(DENSE)
            d_delta = ed.monitor.delta(d_before)
        report["sparse_posting_pages_touched"] = s_delta["posting_pages_touched"]
        report["dense_posting_pages_touched"] = d_delta["posting_pages_touched"]
        report["sparse_bytes_read"] = s_delta["bytes_read"]
        report["dense_bytes_read"] = d_delta["bytes_read"]
        report["checks"]["sparse_touches_fewer_postings"] = (
            s_delta["posting_pages_touched"]
            < d_delta["posting_pages_touched"])

    report["all_passed"] = all(
        v for k, v in report["checks"].items() if isinstance(v, bool))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["all_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
