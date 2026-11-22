"""命令行入口：建索引、计数、分页查询、分面、解释、历史状态台账。

示例
====

.. code-block:: bash

    python -m bitgroup.cli build --index ./idx \\
        --parent-schema '{"region":"text"}' --child-schema '{"temp":"int64","source":"text"}' \\
        --parent parent.jsonl --child child.jsonl
    python -m bitgroup.cli query --index ./idx --ast query.json --state true --page-size 50
    python -m bitgroup.cli facets --index ./idx --ast query.json --attr region
    python -m bitgroup.cli explain --index ./idx --ast query.json --entity-id P

    # 历史状态台账：首次创建（同一条命令随后可用于补录/替换）
    python -m bitgroup.cli ledger-register --ledger ./ledger.db \\
        --date 2026-03-02 --index ./idx_0302 --ast query.json
    python -m bitgroup.cli ledger-register --ledger ./ledger.db \\
        --date 2026-02-28 --index ./idx_0228 --ast query.json
    python -m bitgroup.cli ledger-episodes --ledger ./ledger.db --entity-id P
    python -m bitgroup.cli ledger-sources --ledger ./ledger.db

    # 新旧规则历史对照：两份台账 + 闭合自然日窗口 → JSONL 变更带
    python -m bitgroup.cli ledger-compare --left old.db --right new.db \\
        --start 2026-03-01 --end 2026-03-31 --out band.jsonl

    # 现场复核包：三态名额 + 整组子记录预算 → 三份文件的目录
    python -m bitgroup.cli review-export --index ./idx --ast query.json \\
        --seed s2026 --quota-true 2 --quota-false 1 --quota-unknown 1 \\
        --max-children 500 --out ./review_pkg
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict

from .engine import Engine
from .errors import BitgroupError
from .importer import Importer
from .ledger import Ledger, read_index_schema


def _load_schema(text: str) -> Dict[str, str]:
    val = json.loads(text)
    if not isinstance(val, dict):
        raise ValueError("schema 必须是 JSON 对象")
    return val


def _load_ast(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        ast = json.load(fh)
    if not isinstance(ast, dict):
        raise ValueError("AST 必须是 JSON 对象")
    return ast


def _dump(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="bitgroup")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_build = sub.add_parser("build", help="从 JSONL 建立只读索引")
    p_build.add_argument("--index", required=True)
    p_build.add_argument("--parent-schema", required=True)
    p_build.add_argument("--child-schema", default="{}")
    p_build.add_argument("--parent", required=True)
    p_build.add_argument("--child", default=None)
    p_build.add_argument("--cache-budget-mib", type=int, default=32)

    p_query = sub.add_parser("query", help="三态计数 + 分页 entity_id")
    p_query.add_argument("--index", required=True)
    p_query.add_argument("--ast", required=True)
    p_query.add_argument("--state", default="true",
                         choices=("true", "false", "unknown"))
    p_query.add_argument("--page-size", type=int, default=100)
    p_query.add_argument("--page-token", default=None)
    p_query.add_argument("--cache-budget-mib", type=int, default=32)

    p_facet = sub.add_parser("facets", help="主属性分面（基于 true 集合）")
    p_facet.add_argument("--index", required=True)
    p_facet.add_argument("--ast", required=True)
    p_facet.add_argument("--attr", required=True)
    p_facet.add_argument("--cache-budget-mib", type=int, default=32)

    p_explain = sub.add_parser("explain", help="逐节点解释单个对象")
    p_explain.add_argument("--index", required=True)
    p_explain.add_argument("--ast", required=True)
    p_explain.add_argument("--entity-id", required=True)
    p_explain.add_argument("--cache-budget-mib", type=int, default=32)

    # ---- 历史状态台账 ----------------------------------------------------

    p_reg = sub.add_parser(
        "ledger-register",
        help="登记某业务日期的日结（首次创建 / 补录 / 替换）",
    )
    p_reg.add_argument("--ledger", required=True, help="台账 SQLite 路径")
    p_reg.add_argument("--date", required=True,
                       help="业务日期 YYYY-MM-DD（只取自本参数）")
    p_reg.add_argument("--index", required=True, help="当天的只读索引目录")
    p_reg.add_argument("--ast", required=True, help="规则文件（固定查询 AST）")
    p_reg.add_argument("--cache-budget-mib", type=int, default=32)

    p_epi = sub.add_parser("ledger-episodes", help="读取状态区间")
    p_epi.add_argument("--ledger", required=True)
    p_epi.add_argument("--entity-id", default=None)
    p_epi.add_argument("--state", default=None,
                       choices=("true", "false", "unknown"))
    p_epi.add_argument("--first-day", default=None)
    p_epi.add_argument("--last-day", default=None)
    p_epi.add_argument("--limit", type=int, default=None)
    p_epi.add_argument("--offset", type=int, default=0)

    p_src = sub.add_parser("ledger-sources", help="读取按日来源登记")
    p_src.add_argument("--ledger", required=True)
    p_src.add_argument("--date", default=None)
    p_src.add_argument("--start", default=None)
    p_src.add_argument("--end", default=None)
    p_src.add_argument("--index-token", default=None)
    p_src.add_argument("--limit", type=int, default=None)
    p_src.add_argument("--offset", type=int, default=0)

    p_ods = sub.add_parser("ledger-object-days", help="读取某对象的逐日状态")
    p_ods.add_argument("--ledger", required=True)
    p_ods.add_argument("--entity-id", required=True)
    p_ods.add_argument("--start", default=None)
    p_ods.add_argument("--end", default=None)

    p_stats = sub.add_parser("ledger-stats", help="台账统计")
    p_stats.add_argument("--ledger", required=True)

    p_cmp = sub.add_parser(
        "ledger-compare",
        help="两份台账的只读历史对照：输出按日期压缩的 JSONL 变更带",
    )
    p_cmp.add_argument("--left", required=True, help="左（老规则）台账 SQLite 路径")
    p_cmp.add_argument("--right", required=True, help="右（新规则）台账 SQLite 路径")
    p_cmp.add_argument("--start", required=True,
                       help="窗口首日 YYYY-MM-DD（含）")
    p_cmp.add_argument("--end", required=True, help="窗口末日 YYYY-MM-DD（含）")
    p_cmp.add_argument("--out", default=None,
                       help="JSONL 变更带输出路径（默认标准输出）")
    p_cmp.add_argument("--scratch-dir", default=None,
                       help="磁盘暂存目录（默认系统临时目录）")

    p_rev = sub.add_parser(
        "review-export",
        help="按三态名额与整组子记录预算导出复核包目录",
    )
    p_rev.add_argument("--index", required=True, help="只读索引目录")
    p_rev.add_argument("--ast", required=True, help="规则文件（查询 AST）")
    p_rev.add_argument("--seed", required=True,
                       help="抽样种子（SHA-256 优先级的输入字符串）")
    p_rev.add_argument("--quota-true", type=int, required=True,
                       help="true 类主对象名额（非负整数）")
    p_rev.add_argument("--quota-false", type=int, required=True,
                       help="false 类主对象名额（非负整数）")
    p_rev.add_argument("--quota-unknown", type=int, required=True,
                       help="unknown 类主对象名额（非负整数）")
    p_rev.add_argument("--max-children", type=int, required=True,
                       help="整包允许导出的最大子记录数（非负整数）")
    p_rev.add_argument("--out", required=True,
                       help="输出目录（必须尚不存在；不覆盖）")
    p_rev.add_argument("--cache-budget-mib", type=int, default=32)
    p_rev.add_argument("--scratch-dir", default=None,
                       help="磁盘暂存目录（默认系统临时目录）")

    args = parser.parse_args(argv)
    try:
        if args.cmd == "build":
            imp = Importer(
                args.index,
                _load_schema(args.parent_schema),
                _load_schema(args.child_schema),
                cache_budget_bytes=args.cache_budget_mib * 1024 * 1024,
            )
            handle = imp.run(args.parent, args.child)
            print(json.dumps({
                "index_dir": handle.index_dir,
                "index_token": handle.index_token,
                "meta": handle.meta,
            }, ensure_ascii=False, indent=2))
            return 0

        if args.cmd == "review-export":
            return _run_review_export(args)

        if args.cmd == "ledger-compare":
            return _run_compare(args)

        if args.cmd.startswith("ledger-"):
            return _run_ledger(args)

        with Engine(
            args.index,
            cache_budget_bytes=args.cache_budget_mib * 1024 * 1024,
        ) as eng:
            ast = _load_ast(args.ast)
            if args.cmd == "query":
                res = eng.query(
                    ast, state=args.state, page_size=args.page_size,
                    page_token=args.page_token,
                )
                print(json.dumps({
                    "counts": res.counts,
                    "state": res.state,
                    "page_entity_ids": res.page_entity_ids,
                    "next_token": res.next_token,
                    "fingerprint": res.fingerprint,
                    "io": res.io,
                }, ensure_ascii=False, indent=2))
            elif args.cmd == "facets":
                print(json.dumps(eng.facets(ast, args.attr),
                                 ensure_ascii=False, indent=2))
            else:
                print(json.dumps(eng.explain_entity(ast, args.entity_id),
                                 ensure_ascii=False, indent=2))
        return 0
    except BitgroupError as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc)},
                         ensure_ascii=False), file=sys.stderr)
        return 2
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc)},
                         ensure_ascii=False), file=sys.stderr)
        return 2


def _run_review_export(args) -> int:
    """复核包导出：成品目录写 --out，观测指标写 stderr。"""
    from .review import export_review_package

    ast = _load_ast(args.ast)
    result = export_review_package(
        args.index,
        ast,
        seed=args.seed,
        quota_true=args.quota_true,
        quota_false=args.quota_false,
        quota_unknown=args.quota_unknown,
        max_children=args.max_children,
        out_dir=args.out,
        cache_budget_bytes=args.cache_budget_mib * 1024 * 1024,
        scratch_dir=args.scratch_dir,
    )
    print(json.dumps({
        "ok": True,
        "out_dir": result.out_dir,
        "manifest": result.manifest,
    }, ensure_ascii=False, indent=2))
    print(json.dumps({"metrics": result.metrics}, ensure_ascii=False),
          file=sys.stderr)
    return 0


def _run_compare(args) -> int:
    """只读对照：JSONL 变更带写 --out（默认 stdout），处理量指标写 stderr。"""
    from .compare import compare_ledgers

    with compare_ledgers(args.left, args.right, args.start, args.end,
                         scratch_dir=args.scratch_dir) as run:
        out_fh = (open(args.out, "w", encoding="utf-8") if args.out
                  else sys.stdout)
        try:
            for row in run.rows():
                out_fh.write(json.dumps(row, ensure_ascii=False,
                                        separators=(",", ":")) + "\n")
            metrics = run.metrics
        finally:
            if args.out:
                out_fh.close()
    print(json.dumps({"metrics": metrics}, ensure_ascii=False),
          file=sys.stderr)
    return 0


def _run_ledger(args) -> int:
    with Ledger(args.ledger) as ledger:
        if args.cmd == "ledger-register":
            ast = _load_ast(args.ast)
            parent_schema, child_schema = read_index_schema(args.index)
            if not ledger.exists:
                ledger.create(ast, parent_schema, child_schema)
            # 固定 AST 与 schema 由台账自身核对；日期只取 --date 参数。
            result = ledger.register(
                args.date,
                args.index,
                ast=ast,
                engine_factory=lambda path: Engine(
                    path,
                    cache_budget_bytes=args.cache_budget_mib * 1024 * 1024,
                ),
            )
            _dump({"ok": True, **result.as_dict()})
        elif args.cmd == "ledger-episodes":
            _dump({"episodes": ledger.episodes(
                args.entity_id,
                first_day=args.first_day,
                last_day=args.last_day,
                state=args.state,
                limit=args.limit,
                offset=args.offset,
            )})
        elif args.cmd == "ledger-sources":
            _dump({"daily_sources": ledger.daily_sources(
                business_date=args.date,
                start=args.start,
                end=args.end,
                index_token=args.index_token,
                limit=args.limit,
                offset=args.offset,
            )})
        elif args.cmd == "ledger-object-days":
            _dump({"entity_id": args.entity_id, "days": ledger.object_days(
                args.entity_id, start=args.start, end=args.end)})
        else:
            _dump(ledger.stats())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
