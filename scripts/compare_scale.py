"""历史对照的规模证据：长稳定历史 + 大量零散不可比日下的处理量读数。

现场生成（不入库，用后删除）：

* ``base``：800 个自然日 × 4000 个全程稳定的对象，两边每天同代号登记。
  派生变体把右边每隔 9 天的代号换成另一个（零散未对齐日），分别在
  150 / 300 / 600 / 800 天窗口上对照——稳定对象的区间数与事件数应与
  窗口长度无关，而逐对象逐日展开的记录数随窗口线性增长；
* ``scatter``：同一份 400 日台账复制多份，仅改右边零散日的代号密度
  （无 / 每 40 天 / 每 20 天 / 每 10 天）——零散未对齐日只应增加状态
  游程与输出行，不应放大稳定对象的区间/事件工作量；
* ``churn``：400 天 × (4000 稳定 + 300 每 7 天翻转) 对象，夹杂
  different_source / left_missing / both_missing——工作量由窗口相关的
  状态区间记录决定，并在 128 MiB 地址空间约束下完成；同结构缩小版
  （150 对象 × 120 天）用逐日逐对象枚举全量核对。

报告中的对照量：

* ``entity_days_*``：逐对象逐日展开会物化的记录数（对象数 × 天数）；
* ``segment_intersections``：按可比日段展开会物化的记录数
  （对象数 × 可比段数）；
* ``events_written`` / ``pair_runs``：本实现实际处理的游程端点事件数。

用法::

    PYTHONPATH=. python3 scripts/compare_scale.py --work-dir ./compare_scale_run
"""

from __future__ import annotations

import argparse
import datetime
import gc
import json
import os
import resource
import shutil
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bitgroup.compare import compare_ledgers  # noqa: E402
from bitgroup.importer import Importer  # noqa: E402
from bitgroup.ledger import Ledger  # noqa: E402
from compare_demo import (KIND_CHILDREN, PARENT_SCHEMA,  # noqa: E402
                          CHILD_SCHEMA, OLD_AST, NEW_AST,
                          eval_day, brute_force_band)

DAY0 = datetime.date(2026, 3, 1)


def day(n: int) -> str:
    return (DAY0 + datetime.timedelta(days=n)).isoformat()


def build_index(work, tag, kinds):
    """kinds: {eid: kind}，子行组合见 compare_demo.KIND_CHILDREN。"""
    base = os.path.join(work, "indexes", tag)
    os.makedirs(base, exist_ok=True)
    pp, cp = os.path.join(base, "p.jsonl"), os.path.join(base, "c.jsonl")
    with open(pp, "w", encoding="utf-8") as fh:
        for i, eid in enumerate(kinds):
            fh.write(json.dumps(
                {"entity_id": eid, "zone": "N", "level": i}) + "\n")
    with open(cp, "w", encoding="utf-8") as fh:
        for eid, kind in kinds.items():
            for j, (temp, source) in enumerate(KIND_CHILDREN[kind]):
                fh.write(json.dumps(
                    {"child_id": f"{eid}-{j}", "entity_id": eid,
                     "temp": temp, "source": source}) + "\n")
    idx = os.path.join(base, "index")
    Importer(idx, PARENT_SCHEMA, CHILD_SCHEMA).run(pp, cp)
    return idx


def run_compare(left_db, right_db, start, end):
    with compare_ledgers(left_db, right_db, start, end) as run:
        rows = list(run.rows())
        return rows, run.metrics


def derived(metrics):
    """由实测指标推出“别的实现会物化多少记录”的对照量。"""
    m = metrics
    entities = m["entities_compared"]
    win_days = m["window"]["days"]
    cmp_days = m["status_day_counts"]["comparable"]
    cmp_runs = m["status_run_counts"]["comparable"]
    return {
        "entity_days_window": entities * win_days,
        "entity_days_comparable": entities * cmp_days,
        "segment_intersections": entities * cmp_runs,
    }


def brief(metrics):
    keys = ("daily_source_rows", "episode_rows_in_window",
            "entities_in_window", "vm_steps")
    return {
        "window_days": metrics["window"]["days"],
        **{f"left_{k}": metrics["left"][k] for k in keys},
        **{f"right_{k}": metrics["right"][k] for k in keys},
        "entities_compared": metrics["entities_compared"],
        "pair_runs": metrics["pair_runs"],
        "events_written": metrics["events_written"],
        "event_days": metrics["event_days"],
        "status_runs": metrics["status_runs"],
        "comparable_runs": metrics["status_run_counts"]["comparable"],
        "output_rows": metrics["output_rows"],
        "scratch_vm_steps": metrics["scratch"]["vm_steps"],
        "derived": derived(metrics),
    }


# --------------------------------------------------------------------------
# base：长稳定历史（800 天 × 4000 稳定对象，每天两边同代号）
# --------------------------------------------------------------------------


def gen_base(work, n_entities=4000, n_days=800, reuse=False):
    kinds = {f"E{i:05d}": ("tf", "ft", "TT", "uu")[i % 4]
             for i in range(n_entities)}
    left_db = os.path.join(work, "base_left.db")
    right_db = os.path.join(work, "base_right.db")
    if reuse and os.path.exists(left_db) and os.path.exists(right_db):
        print("  复用已生成的 base 台账", file=sys.stderr)
        return left_db, right_db
    idx = build_index(work, "base_main", kinds)
    with Ledger(left_db) as ll, Ledger(right_db) as rr:
        ll.create(OLD_AST, PARENT_SCHEMA, CHILD_SCHEMA)
        rr.create(NEW_AST, PARENT_SCHEMA, CHILD_SCHEMA)
        for n in range(n_days):
            ll.register(day(n), idx)
            rr.register(day(n), idx)
    return left_db, right_db


def flip_right_tokens(src_db, dst_db, every, n_days):
    """复制台账并把右边每 every 天的代号换掉（模拟两边当天各用各的代号）。

    只改 daily_source 的代号串——与“右边当天登记了同内容的另一个索引”
    在台账层面完全等价，但省去几百次重复登记。
    """
    shutil.copyfile(src_db, dst_db)
    con = sqlite3.connect(dst_db, isolation_level=None)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("BEGIN IMMEDIATE")
        for n in range(0, n_days, every):
            con.execute(
                "UPDATE day_source SET index_token = index_token || '-alt' "
                "WHERE business_date = ?", (day(n),))
        con.execute("COMMIT")
    finally:
        con.close()


def scenario_invariance(work, left_db, right_db):
    """窗口 150/300/600/800 天（都含每 9 天一个不同代号日）。"""
    alt = os.path.join(work, "base_right_alt.db")
    flip_right_tokens(right_db, alt, every=9, n_days=800)
    out = {}
    for n_days in (150, 300, 600, 800):
        _rows, m = run_compare(left_db, alt, day(0), day(n_days - 1))
        out[f"window_{n_days}d"] = brief(m)
    return out


def scenario_scatter(work, left_db, right_db):
    """同一份 400 日窗口，不同密度的零散不同代号日。"""
    out = {}
    for every in (0, 40, 20, 10):
        tag = "none" if every == 0 else f"every_{every}d"
        alt = os.path.join(work, f"scatter_{tag}.db")
        if every == 0:
            shutil.copyfile(right_db, alt)
        else:
            flip_right_tokens(right_db, alt, every=every, n_days=400)
        _rows, m = run_compare(left_db, alt, day(0), day(399))
        out[f"scatter_{tag}"] = brief(m)
    return out


# --------------------------------------------------------------------------
# churn：稳定 + 周期翻转对象，夹杂三种不可比日
# --------------------------------------------------------------------------


def gen_churn(work, tag, n_stable, n_churn, n_days,
              flip_every=7, diff_every=9, both_missing_every=37,
              left_missing_every=53, record_states=False, reuse=False):
    """生成一对台账。record_states=True 时同时返回逐日状态（供枚举核对）。"""
    left_db = os.path.join(work, f"{tag}_left.db")
    right_db = os.path.join(work, f"{tag}_right.db")
    if reuse and os.path.exists(left_db) and os.path.exists(right_db) \
            and not record_states:
        print(f"  复用已生成的 {tag} 台账", file=sys.stderr)
        return left_db, right_db, None
    stable = {f"S{i:05d}": ("tf", "ft", "TT", "uu")[i % 4]
              for i in range(n_stable)}
    churn_ids = [f"C{i:05d}" for i in range(n_churn)]

    def kinds_for(n):
        kinds = dict(stable)
        for j, eid in enumerate(churn_ids):
            kinds[eid] = "tf" if (n // flip_every + j) % 2 == 0 else "ft"
        return kinds

    # 翻转内容只有 (相位奇偶 × 主/备) 几种，按内容去重建索引；
    # 同内容的日子共用同一份索引（同一代号），与逐相位建库在对照层面等价。
    parities = sorted({(n // flip_every) % 2 for n in range(n_days)})
    idx_main = {p: build_index(work, f"{tag}_main_p{p}",
                               kinds_for(p * flip_every)) for p in parities}
    idx_alt = {p: build_index(work, f"{tag}_alt_p{p}",
                              kinds_for(p * flip_every)) for p in parities}

    def idx_for(pool, n):
        return pool[(n // flip_every) % 2]

    left_days, right_days = {}, {}
    src_l, src_r = {}, {}
    with Ledger(left_db) as ll, Ledger(right_db) as rr:
        ll.create(OLD_AST, PARENT_SCHEMA, CHILD_SCHEMA)
        rr.create(NEW_AST, PARENT_SCHEMA, CHILD_SCHEMA)
        for n in range(n_days):
            if both_missing_every and n % both_missing_every == 0:
                continue  # 两边都缺报
            if record_states:
                children = {e: KIND_CHILDREN[k]
                            for e, k in kinds_for(n).items()}
                left_days[day(n)] = eval_day(children, OLD_AST)
                right_days[day(n)] = eval_day(children, NEW_AST)
            if left_missing_every and n % left_missing_every == 0:
                src_r[day(n)] = rr.register(
                    day(n), idx_for(idx_main, n)).index_token
                continue
            if diff_every and n % diff_every == 0:
                src_l[day(n)] = ll.register(
                    day(n), idx_for(idx_main, n)).index_token
                src_r[day(n)] = rr.register(
                    day(n), idx_for(idx_alt, n)).index_token
                continue
            tok_l = ll.register(day(n), idx_for(idx_main, n)).index_token
            tok_r = rr.register(day(n), idx_for(idx_main, n)).index_token
            src_l[day(n)], src_r[day(n)] = tok_l, tok_r
    states = (left_days, right_days, src_l, src_r) if record_states else None
    return left_db, right_db, states


def scenario_churn(work, memory_mib, reuse=False):
    left_db, right_db, _ = gen_churn(
        work, "churn", n_stable=4000, n_churn=300, n_days=400, reuse=reuse)

    # 128 MiB 地址空间约束下完成对照（生成已结束，先 GC 再设限）。
    # 只压低软限制（硬限制调低后非 root 无法恢复），事后还原软限制。
    gc.collect()
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS,
                       (memory_mib * 1024 * 1024, hard))
    try:
        rows, m = run_compare(left_db, right_db, day(0), day(399))
    finally:
        resource.setrlimit(resource.RLIMIT_AS, (soft, hard))
    peak_mib = resource.getrusage(
        resource.RUSAGE_SELF).ru_maxrss / 1024
    out = brief(m)
    out["rows"] = len(rows)
    out["memory_limit_mib"] = memory_mib
    out["peak_rss_mib"] = round(peak_mib, 1)
    return out


def scenario_churn_twin_check(work):
    """同结构缩小版：逐日逐对象枚举全量核对。"""
    left_db, right_db, states = gen_churn(
        work, "twin", n_stable=120, n_churn=30, n_days=120,
        record_states=True)
    left_days, right_days, src_l, src_r = states
    rows, m = run_compare(left_db, right_db, day(0), day(119))
    _want_rows, want_per_day = brute_force_band(
        left_days, right_days, src_l, src_r, day(0), day(119))
    got = {}
    for row in rows:
        a = datetime.date.fromisoformat(row["first_day"]).toordinal()
        b = datetime.date.fromisoformat(row["last_day"]).toordinal()
        for o in range(a, b + 1):
            d = datetime.date.fromordinal(o).isoformat()
            if row["cells"] is None:
                got[d] = (row["status"], None)
                continue
            counts, entered = {}, {}
            has_ent = row["cells"][0]["entered"] is not None
            for c in row["cells"]:
                counts[(c["left"], c["right"])] = c["count"]
                entered[(c["left"], c["right"])] = c["entered"]
            got[d] = (row["status"],
                      (counts, entered if has_ent else None))
    want = {d: (st, cells) for d, (st, cells) in want_per_day.items()}
    ok = got == want
    return {"twin_days_match_enumeration": ok,
            "twin_metrics": brief(m)}


def explain_plans(db_path):
    uri = "file:" + os.path.abspath(db_path) + "?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    try:
        q1 = con.execute(
            "EXPLAIN QUERY PLAN SELECT entity_id, first_ord, last_ord, state "
            "FROM episode_segment WHERE last_ord >= ? AND first_ord <= ? "
            "ORDER BY entity_id, first_ord", (1, 99999999)).fetchall()
        q2 = con.execute(
            "EXPLAIN QUERY PLAN SELECT business_date, index_token "
            "FROM daily_source WHERE business_date >= ? AND business_date <= ? "
            "ORDER BY business_date", ("2026-01-01", "2026-12-31")).fetchall()
    finally:
        con.close()
    return {
        "episode_scan": [r[-1] for r in q1],
        "daily_source_scan": [r[-1] for r in q2],
        "events_aggregation": "GROUP BY + ORDER BY 走 SQLite 排序器"
        "（temp_store=FILE，磁盘暂存）",
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", required=True)
    ap.add_argument("--memory-mib", type=int, default=128,
                    help="churn 场景对照阶段的 RLIMIT_AS 软限制（MiB）")
    ap.add_argument("--keep", action="store_true", help="保留生成的库以便检查")
    ap.add_argument("--reuse", action="store_true",
                    help="复用工作目录里已生成的台账，只重跑对照与核对")
    args = ap.parse_args(argv)
    work = os.path.abspath(args.work_dir)
    if args.reuse and os.path.isdir(work):
        print("复用已有工作目录（--reuse）", file=sys.stderr)
    else:
        shutil.rmtree(work, ignore_errors=True)
        os.makedirs(work)

    report = {"scenarios": {}, "conclusions": {}}

    print("生成 base：800 天 × 4000 稳定对象 ...", file=sys.stderr)
    left_db, right_db = gen_base(work, reuse=args.reuse)

    print("场景 A：窗口长度不变性 ...", file=sys.stderr)
    report["scenarios"]["A_window_length_invariance"] = \
        scenario_invariance(work, left_db, right_db)

    print("场景 B：零散未对齐日密度 ...", file=sys.stderr)
    report["scenarios"]["B_scatter_density"] = \
        scenario_scatter(work, left_db, right_db)

    print("场景 C：稳定+翻转混合、夹杂不可比日（含内存约束） ...",
          file=sys.stderr)
    report["scenarios"]["C_churn_with_missing_days"] = \
        scenario_churn(work, args.memory_mib, reuse=args.reuse)

    print("场景 C 缩小版枚举核对 ...", file=sys.stderr)
    report["scenarios"]["C_twin_brute_force"] = \
        scenario_churn_twin_check(work)

    report["query_plans"] = explain_plans(left_db)

    a = report["scenarios"]["A_window_length_invariance"]
    b = report["scenarios"]["B_scatter_density"]
    c = report["scenarios"]["C_churn_with_missing_days"]
    twin = report["scenarios"]["C_twin_brute_force"]

    a150, a800 = a["window_150d"], a["window_800d"]
    report["conclusions"] = {
        # A：窗口 ×5.3，稳定对象的区间/事件工作量不变；逐日展开量 ×5.3
        "A_interval_work_independent_of_window_length": (
            a150["pair_runs"] == a800["pair_runs"]
            and a150["events_written"] == a800["events_written"]
            and a800["derived"]["entity_days_window"]
            >= 5 * a150["derived"]["entity_days_window"]),
        # A：按可比段展开会比本实现多放大一个数量级
        "A_segment_expansion_would_amplify": (
            a800["derived"]["segment_intersections"]
            >= 10 * a800["events_written"]),
        # B：零散未对齐日密度 ×∞，稳定对象的区间/事件工作量不变
        "B_scatter_does_not_amplify_stable_objects": all(
            b[k]["pair_runs"] == b["scatter_none"]["pair_runs"]
            and b[k]["events_written"] == b["scatter_none"]["events_written"]
            for k in b),
        # B：密度上升只增加状态游程与输出行
        "B_scatter_only_adds_status_runs": (
            b["scatter_every_10d"]["status_runs"]
            > b["scatter_every_20d"]["status_runs"]
            > b["scatter_every_40d"]["status_runs"]
            > b["scatter_none"]["status_runs"]),
        # C：实际处理量是游程端点事件（≈2×游程数），且游程数远小于
        #    逐对象逐日展开量——工作量由区间记录决定，而非对象×天数
        "C_events_track_intervals_not_entity_days": (
            c["events_written"]
            <= 2 * c["pair_runs"] + c["entities_compared"]
            and c["pair_runs"] * 10
            < c["derived"]["entity_days_comparable"]),
        "C_compare_within_memory_limit": (
            c["peak_rss_mib"] < c["memory_limit_mib"]),
        "C_twin_matches_brute_force":
            twin["twin_days_match_enumeration"],
    }
    report["ok"] = all(report["conclusions"].values())
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not args.keep:
        shutil.rmtree(work, ignore_errors=True)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
