"""新旧规则历史对照：小样本复跑 + 逐日逐对象独立核对。

从空目录开始，走真实管线（Importer 建索引 → Ledger.register 两套规则各记
一份台账 → ledger-compare 出变更带），再用**独立实现**（直接读原始 JSONL
逐日判定、逐对象枚举九格）核对变更带解压后的每一天以及压缩边界。

演示样本（业务日期 2026-04-01 .. 2026-04-10，对照窗口 04-02 .. 04-09）：

* 04-02 → 04-03：甲、乙在 (false,true) / (true,false) 两格互换——两格人数
  仍各为 1，次日两格各进入 1 人；
* 04-04：两边用不同代号登记同内容日报（各格人数恰好不变）→ 当天无比较
  结果，04-05 的 entered 不可计算（null，不是零）；
* 04-06 只有右边登记（left_missing）、04-08 两边都缺报（both_missing）；
* 戊（unknown,false）04-03 进入、04-07 前离开；己（unknown,true）04-07
  进入；丁（unknown,unknown）04-07 前离开——覆盖 unknown 与对象出入；
* 窗口两端裁剪：04-01、04-10 已登记但在窗口外，04-02 的 entered 因前一
  日在窗口外而为 null。

用法::

    PYTHONPATH=. python3 scripts/compare_demo.py --work-dir ./compare_run

工作目录与产物按 .gitignore 排除，不提交进仓库。
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import shutil
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bitgroup.compare import CELLS, compare_ledgers  # noqa: E402
from bitgroup.importer import Importer  # noqa: E402
from bitgroup.ledger import Ledger  # noqa: E402

PARENT_SCHEMA = {"zone": "text", "level": "int64"}
CHILD_SCHEMA = {"temp": "int64", "source": "text"}


def _rule(source_value):
    """zone==N 且存在 (temp<25 且 source==source_value) 的子行。"""
    return {
        "op": "and",
        "args": [
            {"op": "cmp", "domain": "parent", "attr": "zone",
             "op_kind": "eq", "value": "N"},
            {"op": "exists", "arg": {"op": "and", "args": [
                {"op": "cmp", "domain": "child", "attr": "temp",
                 "op_kind": "lt", "value": 25},
                {"op": "cmp", "domain": "child", "attr": "source",
                 "op_kind": "eq", "value": source_value},
            ]}},
        ],
    }


OLD_AST = _rule("X")  # 老规则
NEW_AST = _rule("Y")  # 新规则

DAY0 = datetime.date(2026, 4, 1)


def day(n: int) -> str:
    return (DAY0 + datetime.timedelta(days=n)).isoformat()


def ord_of(text: str) -> int:
    return datetime.date.fromisoformat(text).toordinal()


# --------------------------------------------------------------------------
# 逐日内容（子行组合），kind -> [(temp, source), ...]
# --------------------------------------------------------------------------

KIND_CHILDREN = {
    "tf": [(10, "X")],                    # (老 true,  新 false)
    "ft": [(10, "Y")],                    # (老 false, 新 true)
    "TT": [(10, "X"), (10, "Y")],         # (老 true,  新 true)
    "uf": [(None, "X")],                  # (老 unknown, 新 false)
    "ut": [(None, "X"), (10, "Y")],       # (老 unknown, 新 true)
    "uu": [(None, "X"), (None, "Y")],     # (老 unknown, 新 unknown)
}

BASE = {"甲": "ft", "乙": "tf", "丙": "TT", "丁": "uu"}
SWAPPED = {"甲": "tf", "乙": "ft", "丙": "TT", "丁": "uu", "戊": "uf"}
LATER = {"甲": "tf", "乙": "ft", "丙": "TT", "己": "ut"}

# 每天的内容；04-06 只有右边登记，04-08 两边都缺报
CONTENT = {
    day(0): BASE,
    day(1): BASE,
    day(2): SWAPPED,     # 甲乙互换；戊进入
    day(3): SWAPPED,     # 同内容，但两边各用各的代号
    day(4): SWAPPED,
    day(5): {"甲": "tf", "乙": "ft", "丙": "TT"},
    day(6): LATER,       # 丁、戊离开；己进入
    day(8): LATER,
    day(9): LATER,
}
WINDOW_START, WINDOW_END = day(1), day(8)


# --------------------------------------------------------------------------
# 独立参考实现（不复用对照/台账的任何逻辑）
# --------------------------------------------------------------------------


def eval_day(children_by_entity, ast):
    """直接对子行组合逐对象算三态。只支持本样本用的 and/cmp/exists 结构。"""

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

    def eval_node(node, prow, rows):
        op = node["op"]
        if op == "cmp":
            return cmp(prow if node["domain"] == "parent" else rows, node)
        if op == "and":
            return and3([eval_node(a, prow, rows) for a in node["args"]])
        if op == "exists":
            sts = [eval_node(node["arg"], prow, r) for r in rows]
            if "true" in sts:
                return "true"
            return "unknown" if "unknown" in sts else "false"
        raise ValueError(op)

    out = {}
    for eid, children in children_by_entity.items():
        prow = {"entity_id": eid, "zone": "N"}
        out[eid] = eval_node(ast, prow, [
            {"temp": t, "source": s} for t, s in children])
    return out


CELLS9 = list(CELLS)


def brute_force_band(left_days, right_days, src_left, src_right, start, end):
    """逐日逐对象枚举九格，再按规则压缩。完全独立于被测实现。"""
    s, e = ord_of(start), ord_of(end)
    per_day = {}
    prev_status = None
    for o in range(s, e + 1):
        d = datetime.date.fromordinal(o).isoformat()
        lt, rt = src_left.get(d), src_right.get(d)
        if lt is not None and rt is not None:
            status = "comparable" if lt == rt else "different_source"
        elif lt is None and rt is None:
            status = "both_missing"
        elif lt is None:
            status = "left_missing"
        else:
            status = "right_missing"
        cells = None
        if status == "comparable":
            lm, rm = left_days.get(d, {}), right_days.get(d, {})
            counts = {c: 0 for c in CELLS9}
            for eid in lm.keys() & rm.keys():
                counts[(lm[eid], rm[eid])] += 1
            entered = None
            if o - 1 >= s and prev_status == "comparable":
                pl = left_days.get(datetime.date.fromordinal(o - 1)
                                   .isoformat(), {})
                pr = right_days.get(datetime.date.fromordinal(o - 1)
                                    .isoformat(), {})
                entered = {c: 0 for c in CELLS9}
                for eid in lm.keys() & rm.keys():
                    cell = (lm[eid], rm[eid])
                    if (eid not in pl or eid not in pr
                            or (pl[eid], pr[eid]) != cell):
                        entered[cell] += 1
            cells = (counts, entered)
        per_day[d] = (status, cells)
        prev_status = status
    # 压缩：仅合并 status 与整组 cells 全同的相邻自然日
    rows = []
    for d in sorted(per_day, key=ord_of):
        status, cells = per_day[d]
        if (rows and rows[-1]["status"] == status
                and rows[-1]["cells"] == cells):
            rows[-1]["last_day"] = d
        else:
            rows.append({"first_day": d, "last_day": d,
                         "status": status, "cells": cells})
    return rows, per_day


def canon_row_cells(row):
    """命令输出的一行 -> 与独立参考相同的 cells 结构。"""
    if row["cells"] is None:
        return None
    counts = {}
    entered = {}
    has_entered = row["cells"][0]["entered"] is not None
    for c in row["cells"]:
        counts[(c["left"], c["right"])] = c["count"]
        entered[(c["left"], c["right"])] = c["entered"]
    return (counts, entered if has_entered else None)


# --------------------------------------------------------------------------
# 管线搭建
# --------------------------------------------------------------------------


def build_index(work, tag, present):
    base = os.path.join(work, "indexes", tag)
    os.makedirs(base, exist_ok=True)
    rows_p, rows_c = [], []
    for i, (eid, kind) in enumerate(present.items()):
        rows_p.append({"entity_id": eid, "zone": "N", "level": i})
        for j, (temp, source) in enumerate(KIND_CHILDREN[kind]):
            rows_c.append({"child_id": f"{eid}-{j}", "entity_id": eid,
                           "temp": temp, "source": source})
    pp, cp = os.path.join(base, "p.jsonl"), os.path.join(base, "c.jsonl")
    with open(pp, "w", encoding="utf-8") as fh:
        for r in rows_p:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    with open(cp, "w", encoding="utf-8") as fh:
        for r in rows_c:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    idx = os.path.join(base, "index")
    Importer(idx, PARENT_SCHEMA, CHILD_SCHEMA).run(pp, cp)
    return idx


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", required=True)
    ap.add_argument("--keep", action="store_true", help="保留生成的库以便检查")
    args = ap.parse_args(argv)
    work = os.path.abspath(args.work_dir)
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)

    left_db = os.path.join(work, "old_rule.db")
    right_db = os.path.join(work, "new_rule.db")

    # 1) 建索引：04-04（day(3)）左右各一份同内容索引（不同代号），
    #    04-06（day(5)）只有右边一份
    idx = {d: build_index(work, f"i_{d}", c) for d, c in CONTENT.items()}
    idx_alt = build_index(work, "i_alt_right", CONTENT[day(3)])

    # 2) 两套规则各记一份台账；记录每天的来源代号与独立判定的日状态
    src_l, src_r = {}, {}
    left_days, right_days = {}, {}
    with Ledger(left_db) as ll, Ledger(right_db) as rr:
        ll.create(OLD_AST, PARENT_SCHEMA, CHILD_SCHEMA)
        rr.create(NEW_AST, PARENT_SCHEMA, CHILD_SCHEMA)
        for d, content in CONTENT.items():
            children = {e: KIND_CHILDREN[k] for e, k in content.items()}
            old_map = eval_day(children, OLD_AST)
            new_map = eval_day(children, NEW_AST)
            if d == day(5):
                src_r[d] = rr.register(d, idx[d]).index_token
                right_days[d] = new_map
                continue
            if d == day(3):
                src_l[d] = ll.register(d, idx[d]).index_token
                src_r[d] = rr.register(d, idx_alt).index_token
            else:
                src_l[d] = ll.register(d, idx[d]).index_token
                src_r[d] = rr.register(d, idx[d]).index_token
            left_days[d] = old_map
            right_days[d] = new_map

    # 3) 对照命令（Python API）+ 独立枚举核对
    with compare_ledgers(left_db, right_db,
                         WINDOW_START, WINDOW_END) as run:
        band = list(run.rows())
        metrics = run.metrics

    want_rows, want_per_day = brute_force_band(
        left_days, right_days, src_l, src_r, WINDOW_START, WINDOW_END)

    checks = {}

    # 3a) 解压后逐日相等
    got_per_day = {}
    prev_end = None
    tiling_ok = True
    for row in band:
        a, b = ord_of(row["first_day"]), ord_of(row["last_day"])
        if prev_end is not None and a != prev_end + 1:
            tiling_ok = False
        prev_end = b
        for o in range(a, b + 1):
            got_per_day[datetime.date.fromordinal(o).isoformat()] = (
                row["status"], canon_row_cells(row))
    want_flat = {d: (st, cells) for d, (st, cells) in want_per_day.items()}
    checks["decompressed_days_match_enumeration"] = got_per_day == want_flat
    checks["window_tiling_no_gap_no_overlap"] = (
        tiling_ok
        and min(ord_of(d) for d in got_per_day) == ord_of(WINDOW_START)
        and max(ord_of(d) for d in got_per_day) == ord_of(WINDOW_END))

    # 3b) 压缩边界：与独立压缩完全一致，且不存在可再合并的相邻段
    got_compressed = [
        (r["first_day"], r["last_day"], r["status"], canon_row_cells(r))
        for r in band
    ]
    want_compressed = [
        (r["first_day"], r["last_day"], r["status"], r["cells"])
        for r in want_rows
    ]
    checks["compression_boundaries_match"] = (
        got_compressed == want_compressed)
    mergeable = any(
        a["status"] == b["status"]
        and canon_row_cells(a) == canon_row_cells(b)
        for a, b in zip(band, band[1:]))
    checks["no_mergeable_adjacent_rows"] = not mergeable

    # 3c) 题目点名的关键点位
    def row_at(d):
        for r in band:
            if ord_of(r["first_day"]) <= ord_of(d) <= ord_of(r["last_day"]):
                return r
        raise AssertionError(d)

    def cell(row, l, r):
        return next(c for c in row["cells"]
                    if c["left"] == l and c["right"] == r)

    r_swap = row_at(day(2))
    checks["swap_day_counts_stay_one_each"] = (
        cell(r_swap, "false", "true")["count"] == 1
        and cell(r_swap, "true", "false")["count"] == 1)
    checks["swap_day_each_cell_entered_one"] = (
        cell(r_swap, "false", "true")["entered"] == 1
        and cell(r_swap, "true", "false")["entered"] == 1)
    checks["different_source_day_has_no_cells"] = (
        row_at(day(3))["status"] == "different_source"
        and row_at(day(3))["cells"] is None)
    r_after = row_at(day(4))
    checks["day_after_different_source_entered_null"] = (
        r_after["status"] == "comparable"
        and all(c["entered"] is None for c in r_after["cells"]))
    checks["window_first_day_entered_null"] = all(
        c["entered"] is None for c in row_at(WINDOW_START)["cells"])
    checks["statuses"] = (
        row_at(day(5))["status"] == "left_missing"
        and row_at(day(7))["status"] == "both_missing")

    # 4) 同一份台账走命令行：JSONL 与 API 一致，指标在 stderr
    env = dict(os.environ)
    env["PYTHONPATH"] = os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))
    band_path = os.path.join(work, "band.jsonl")
    proc = subprocess.run(
        [sys.executable, "-m", "bitgroup.cli", "ledger-compare",
         "--left", left_db, "--right", right_db,
         "--start", WINDOW_START, "--end", WINDOW_END,
         "--out", band_path],
        env=env, capture_output=True, text=True)
    checks["cli_exit_zero"] = proc.returncode == 0
    with open(band_path, encoding="utf-8") as fh:
        cli_rows = [json.loads(x) for x in fh if x.strip()]
    checks["cli_jsonl_matches_api"] = cli_rows == json.loads(
        json.dumps(band, ensure_ascii=False))
    cli_metrics = json.loads(proc.stderr)["metrics"]
    checks["cli_metrics_match_api"] = (
        cli_metrics["output_rows"] == metrics["output_rows"]
        and cli_metrics["pair_runs"] == metrics["pair_runs"])

    ok = all(checks.values())
    report = {
        "ok": ok,
        "checks": checks,
        "window": [WINDOW_START, WINDOW_END],
        "band": band,
        "metrics": metrics,
        "work_dir": work,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not args.keep:
        shutil.rmtree(work, ignore_errors=True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
