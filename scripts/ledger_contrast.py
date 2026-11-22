"""历史对照实验：证明“同日更正”的更新代价与旧历史长度无关。

生成两份可复现的历史（不入库，用后删除）：

* ``same_correction``：**同样的一天更正**，但无关历史长度不同
  （10 个 vs 1000 个无关对象，各 120 天），读取区间维护的 VM 步数、
  查找语句数与读到的区间行数；
* ``long_segment``：被更正对象自身处于**长历史同态段**（段长 5 天 vs
  300 天），在同一位置打断，读取调整代价。

所有读数由 SQLite 真实执行得到：``episode_vm_steps`` 来自
``set_progress_handler`` 计数，``episode_lookup_statements`` /
``episode_rows_read`` 来自区间维护路径的实际调用，``episode_rows_deleted``
等来自实际写入。若实现扫描了整条历史或全表，这些读数会随历史长度增长。

用法::

    PYTHONPATH=. python3 scripts/ledger_contrast.py --work-dir ./contrast_run

产物不入库（``contrast_run/`` 已被 .gitignore 覆盖为通用临时目录之外，
脚本会在结束时删除）。
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bitgroup.importer import Importer  # noqa: E402
from bitgroup.ledger import Ledger  # noqa: E402

PARENT_SCHEMA = {"zone": "text", "level": "int64"}
CHILD_SCHEMA = {"temp": "int64", "source": "text"}
AST = {
    "op": "and",
    "args": [
        {"op": "cmp", "domain": "parent", "attr": "zone",
         "op_kind": "eq", "value": "N"},
        {"op": "exists", "arg": {"op": "and", "args": [
            {"op": "cmp", "domain": "child", "attr": "temp",
             "op_kind": "lt", "value": 25},
            {"op": "cmp", "domain": "child", "attr": "source",
             "op_kind": "eq", "value": "X"},
        ]}},
    ],
}


def _write(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")


def _build(work, tag, present):
    base = os.path.join(work, "idx", tag)
    os.makedirs(base, exist_ok=True)
    pp, cp = os.path.join(base, "p.jsonl"), os.path.join(base, "c.jsonl")
    parents, children = [], []
    for i, (eid, kind) in enumerate(present.items()):
        parents.append({"entity_id": eid, "zone": "N", "level": i})
        if kind == "true":
            children.append({"child_id": f"{eid}-t", "entity_id": eid,
                             "temp": 10, "source": "X"})
        elif kind == "false":
            children.append({"child_id": f"{eid}-f", "entity_id": eid,
                             "temp": 10, "source": "Y"})
        elif kind == "unknown":
            children.append({"child_id": f"{eid}-u", "entity_id": eid,
                             "temp": None, "source": "X"})
    _write(pp, parents)
    _write(cp, children)
    idx = os.path.join(base, "index")
    Importer(idx, PARENT_SCHEMA, CHILD_SCHEMA).run(pp, cp)
    return idx


def _history(work, tag, n_days, n_unrelated, target_day, target_state):
    """建 n_days 天历史，并在 target_day 用 target_state 覆盖 TARGET。"""
    db = os.path.join(work, f"{tag}.db")
    hist_metrics = []
    with Ledger(db) as L:
        L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
        for i in range(n_days):
            present = {"TARGET": "true"}
            for k in range(n_unrelated):
                present[f"U{k}"] = "true" if (k + i) % 3 else "false"
            L.register(f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}", _build(
                work, f"{tag}_h{i}", present))
        # 同日更正
        present = {"TARGET": target_state}
        for k in range(n_unrelated):
            present[f"U{k}"] = "true" if (k + target_day) % 3 else "false"
        r = L.register(
            f"2026-{1 + target_day // 28:02d}-{1 + target_day % 28:02d}",
            _build(work, f"{tag}_fix", present))
        hist_metrics.append(r)
    return db, r


def run(work):
    os.makedirs(work)

    # 对照一：同样的一天更正，不同长度的无关历史
    _, r_small = _history(work, "small", n_days=120, n_unrelated=10,
                          target_day=60, target_state="false")
    _, r_big = _history(work, "big", n_days=120, n_unrelated=1000,
                        target_day=60, target_state="false")

    # 对照二：被更正对象自身的同态段长度不同
    _, r_short_seg = _history(work, "shortseg", n_days=6, n_unrelated=0,
                              target_day=5, target_state="false")
    _, r_long_seg = _history(work, "longseg", n_days=300, n_unrelated=0,
                             target_day=299, target_state="false")

    def view(r):
        m = r.metrics
        return {
            "changed_entities": r.changed_entities,
            "episode_adjustments": r.episode_adjustments,
            "episode_lookup_statements": m.episode_lookup_statements,
            "episode_rows_read": m.episode_rows_read,
            "episode_rows_inserted": m.episode_rows_inserted,
            "episode_rows_deleted": m.episode_rows_deleted,
            "episode_vm_steps": m.episode_vm_steps,
            "episode_vm_granularity": m.episode_vm_granularity,
        }

    report = {
        "same_correction_diff_unrelated_history": {
            "unrelated_10": view(r_small),
            "unrelated_1000": view(r_big),
        },
        "same_correction_diff_target_segment_length": {
            "segment_5_days": view(r_short_seg),
            "segment_300_days": view(r_long_seg),
        },
        "conclusion": {},
    }
    a, b = view(r_small), view(r_big)
    c, d = view(r_short_seg), view(r_long_seg)
    report["conclusion"] = {
        "unrelated_history_does_not_change_update_cost": (
            a["episode_vm_steps"] == b["episode_vm_steps"]
            and a["episode_lookup_statements"] == b["episode_lookup_statements"]
            and a["episode_rows_read"] == b["episode_rows_read"]
        ),
        "target_segment_length_does_not_change_update_cost": (
            c["episode_vm_steps"] == d["episode_vm_steps"]
            and c["episode_lookup_statements"] == d["episode_lookup_statements"]
        ),
    }
    return report


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", required=True)
    ap.add_argument("--keep", action="store_true", help="保留生成的库以便检查")
    args = ap.parse_args(argv)
    work = os.path.abspath(args.work_dir)
    shutil.rmtree(work, ignore_errors=True)
    try:
        report = run(work)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        ok = all(report["conclusion"].values())
        return 0 if ok else 1
    finally:
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
