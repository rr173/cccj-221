"""历史状态台账小夹具复跑：从空库逐日登记、乱序补报、历史更正、重送，
并用**独立实现**（直接读原始 JSONL 逐日判定、再按自然日合并）核对整张
区间表与每日来源登记。不复用台账的任何更新逻辑。

演示步骤（固定查询规则见 fixtures/ledger/query.json）：

1. 登记 2026-03-02（day01）与 2026-03-05（day03），中间 03-03 / 03-04 缺报；
2. 乱序补报 03-03（day02）：同一对象行序不同，且当日日报不含 C；
3. 只换来源、不改判定：把 day02 重新导入为新索引代号再登记 03-03；
4. 历史更正：用 day02_corrected 替换 03-03，移除 A；
5. 重送：同一天同一索引代号再次登记，应为幂等。

用法::

    PYTHONPATH=. python3 scripts/ledger_demo.py --work-dir ./ledger_run

工作目录、索引与台账产物均按 .gitignore 排除，不提交进仓库。
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bitgroup.importer import Importer  # noqa: E402
from bitgroup.ledger import Ledger  # noqa: E402

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIX = os.path.join(HERE, "fixtures", "ledger")


# --------------------------------------------------------------------------
# 独立核对：完全独立于台账的更新逻辑
# --------------------------------------------------------------------------


def load_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if raw:
                rows.append(json.loads(raw))
    return rows


def eval_day(parent_path, child_path, ast):
    """逐对象算三态，返回 {entity_id: state}。纯 Python 参考实现。"""
    parents = load_rows(parent_path)
    children = load_rows(child_path)
    by_entity = {}
    for ch in children:
        by_entity.setdefault(ch["entity_id"], []).append(ch)

    def cmp(row, node):
        v = row.get(node["attr"])
        lit = node["value"]
        if v is None or lit is None:
            return "unknown"
        ok = {"eq": v == lit, "ne": v != lit, "lt": v < lit,
              "le": v <= lit, "gt": v > lit, "ge": v >= lit}[node["op_kind"]]
        return "true" if ok else "false"

    def and3(ss):
        return ("false" if "false" in ss else
                "unknown" if "unknown" in ss else "true")

    def or3(ss):
        return ("true" if "true" in ss else
                "unknown" if "unknown" in ss else "false")

    def not3(s):
        return {"true": "false", "false": "true", "unknown": "unknown"}[s]

    def eval_child(node, row):
        op = node["op"]
        if op == "cmp":
            return cmp(row, node)
        if op == "and":
            return and3([eval_child(a, row) for a in node["args"]])
        if op == "or":
            return or3([eval_child(a, row) for a in node["args"]])
        if op == "not":
            return not3(eval_child(node["arg"], row))
        raise ValueError(op)

    def lift(node, eid):
        sts = [eval_child(node["arg"], r) for r in by_entity.get(eid, [])]
        t, u = sts.count("true"), sts.count("unknown")
        if node["op"] == "exists":
            return "true" if t >= 1 else ("unknown" if t + u >= 1 else "false")
        k = node["k"]
        if t >= k:
            return "true"
        if t + u < k:
            return "false"
        return "unknown"

    def eval_parent(node, eid, prow):
        op = node["op"]
        if op == "cmp":
            return cmp(prow, node)
        if op == "and":
            return and3([eval_parent(a, eid, prow) for a in node["args"]])
        if op == "or":
            return or3([eval_parent(a, eid, prow) for a in node["args"]])
        if op == "not":
            return not3(eval_parent(node["arg"], eid, prow))
        return lift(node, eid)

    return {p["entity_id"]: eval_parent(ast, p["entity_id"], p)
            for p in parents}


def independent_episodes(day_states):
    """day_states: {date_text: {eid: state}} -> 连续同态段集合。"""
    per = {}
    for date_text, dm in day_states.items():
        o = datetime.date.fromisoformat(date_text).toordinal()
        for eid, st in dm.items():
            per.setdefault(eid, {})[o] = st
    out = set()
    for eid, dm in per.items():
        for o in sorted(dm):
            st = dm[o]
            if dm.get(o - 1) == st:
                continue
            j = o
            while dm.get(j + 1) == st:
                j += 1
            out.add((eid, datetime.date.fromordinal(o).isoformat(),
                     datetime.date.fromordinal(j).isoformat(), st))
    return out


def ledger_episodes(db):
    import sqlite3
    con = sqlite3.connect(db)
    try:
        return {(r[0], r[1], r[2], r[3]) for r in con.execute(
            "SELECT entity_id, first_day, last_day, state FROM episode")}
    finally:
        con.close()


def ledger_sources(db):
    import sqlite3
    con = sqlite3.connect(db)
    try:
        return {r[0]: r[1] for r in con.execute(
            "SELECT business_date, index_token FROM daily_source")}
    finally:
        con.close()


# --------------------------------------------------------------------------


def build_index(work, tag, day_dir):
    idx = os.path.join(work, "indexes", tag)
    os.makedirs(os.path.dirname(idx), exist_ok=True)
    Importer(idx, {"region": "text", "level": "int64"},
             {"temp": "int64", "source": "text"}).run(
        os.path.join(day_dir, "parent.jsonl"),
        os.path.join(day_dir, "child.jsonl"))
    return idx


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", required=True)
    args = ap.parse_args(argv)
    work = os.path.abspath(args.work_dir)
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)

    ast = json.load(open(os.path.join(FIX, "query.json"), encoding="utf-8"))
    schema = json.load(open(os.path.join(FIX, "schema.json"), encoding="utf-8"))
    db = os.path.join(work, "ledger.db")

    day_states = {}
    sources = {}
    trace = []

    def snapshot(tag):
        trace.append({
            "step": tag,
            "episodes": [list(x) for x in sorted(ledger_episodes(db))],
            "sources": {
                d: t[:8] for d, t in sorted(ledger_sources(db).items())
            },
        })

    with Ledger(db) as L:
        L.create(ast, schema["parent"], schema["child"])

        # 1. 03-02 与 03-05；03-03/03-04 缺报
        idx_mon = build_index(work, "day01", os.path.join(FIX, "day01"))
        r = L.register("2026-03-02", idx_mon)
        day_states["2026-03-02"] = eval_day(
            os.path.join(FIX, "day01", "parent.jsonl"),
            os.path.join(FIX, "day01", "child.jsonl"), ast)
        sources["2026-03-02"] = r.index_token
        idx_thu = build_index(work, "day03", os.path.join(FIX, "day03"))
        r = L.register("2026-03-05", idx_thu)
        day_states["2026-03-05"] = eval_day(
            os.path.join(FIX, "day03", "parent.jsonl"),
            os.path.join(FIX, "day03", "child.jsonl"), ast)
        sources["2026-03-05"] = r.index_token
        snapshot("1: register 03-02 & 03-05 (03-03/03-04 missing)")

        # 2. 乱序补报 03-03（行序不同，且不含 C）
        idx_tue = build_index(work, "day02", os.path.join(FIX, "day02"))
        r = L.register("2026-03-03", idx_tue)
        day_states["2026-03-03"] = eval_day(
            os.path.join(FIX, "day02", "parent.jsonl"),
            os.path.join(FIX, "day02", "child.jsonl"), ast)
        sources["2026-03-03"] = r.index_token
        snapshot("2: backfill 03-03 (row order changed, C absent)")

        # 3. 只换来源、不改判定：day02 重新导入为新索引代号
        idx_tue_b = build_index(work, "day02_copy",
                                os.path.join(FIX, "day02"))
        before_eps = ledger_episodes(db)
        r_src = L.register("2026-03-03", idx_tue_b)
        assert ledger_episodes(db) == before_eps, "只换来源不应改变区间"
        assert r_src.changed_entities == 0
        sources["2026-03-03"] = r_src.index_token
        snapshot("3: source-only change on 03-03 (same verdict)")

        # 4. 历史更正：03-03 移除 A
        idx_tue_c = build_index(work, "day02_corrected",
                                os.path.join(FIX, "day02_corrected"))
        r = L.register("2026-03-03", idx_tue_c)
        day_states["2026-03-03"] = eval_day(
            os.path.join(FIX, "day02_corrected", "parent.jsonl"),
            os.path.join(FIX, "day02_corrected", "child.jsonl"), ast)
        sources["2026-03-03"] = r.index_token
        snapshot("4: historical correction on 03-03 (A removed)")

        # 5. 重送：同一天同一索引代号再送一次，应为幂等
        before_eps = ledger_episodes(db)
        r_resend = L.register("2026-03-03", idx_tue_c)
        assert ledger_episodes(db) == before_eps, "重送不应改变区间"
        assert r_resend.changed_entities == 0
        snapshot("5: resend same 03-03 token (idempotent)")

        got = ledger_episodes(db)
        got_src = ledger_sources(db)

    want = independent_episodes(day_states)
    ok = (got == want) and (got_src == sources)
    report = {
        "ok": ok,
        "day_states": day_states,
        "trace": trace,
        "final_episodes": [list(x) for x in sorted(got)],
        "independent_episodes": [list(x) for x in sorted(want)],
        "mismatch_episodes": [list(x) for x in sorted(want ^ got)],
        "sources_match_independent": got_src == sources,
        "resend_changed_entities": r_resend.changed_entities,
        "work_dir": work,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
