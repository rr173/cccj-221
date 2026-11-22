"""命令行入口：建索引、计数、分页查询、分面、解释。

示例
====

.. code-block:: bash

    python -m bitgroup.cli build --index ./idx \\
        --parent-schema '{"region":"text"}' --child-schema '{"temp":"int64","source":"text"}' \\
        --parent parent.jsonl --child child.jsonl
    python -m bitgroup.cli query --index ./idx --ast query.json --state true --page-size 50
    python -m bitgroup.cli facets --index ./idx --ast query.json --attr region
    python -m bitgroup.cli explain --index ./idx --ast query.json --entity-id P
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict

from .engine import Engine
from .errors import BitgroupError
from .importer import Importer


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


if __name__ == "__main__":
    raise SystemExit(main())
