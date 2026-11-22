"""历史对照命令（ledger-compare / compare_ledgers）的端到端测试。

覆盖题目要求的验收点：

* 甲乙互换：相邻可比日两格人数不变、次日两格各进入 1 人；
* 中间某日两边用不同代号（各格人数恰好不变）：该天无比较结果，紧随其后
  的可比日 entered 不可计算（null，不是零）；
* left_missing / right_missing / both_missing；整窗无日报时只有一个
  both_missing 段；
* 只认当天代号：同一代号出现在别的日期不算；规则指纹不同不妨碍可比；
* unknown 三态、对象出入、缺席不算 unknown、窗口裁剪；
* 空批次同源日可比（九格全零）；相邻同态日必须合并；
* 只读：台账文件不被修改；写者并发提交时读视图仍是同一份快照；
* 小规模全管线（Importer + Ledger.register）+ 逐日逐对象直接枚举核对。

运行：``PYTHONPATH=/workspace python3 -m unittest tests.test_compare -v``
"""

from __future__ import annotations

import datetime
import hashlib
import io
import json
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr

from bitgroup import cli
from bitgroup.compare import CELLS, compare_ledgers
from bitgroup.errors import LedgerError
from bitgroup.importer import Importer
from bitgroup.ledger import Ledger

STATES = ("true", "false", "unknown")
CELLS9 = [(l, r) for l in STATES for r in STATES]

PARENT_SCHEMA = {"zone": "text", "level": "int64"}
CHILD_SCHEMA = {"temp": "int64", "source": "text"}
# 老规则：zone==N 且存在 (temp<25 且 source==X) 的子行
OLD_AST = {
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
# 新规则：source 要求改为 Y
NEW_AST = {
    "op": "and",
    "args": [
        {"op": "cmp", "domain": "parent", "attr": "zone",
         "op_kind": "eq", "value": "N"},
        {"op": "exists", "arg": {"op": "and", "args": [
            {"op": "cmp", "domain": "child", "attr": "temp",
             "op_kind": "lt", "value": 25},
            {"op": "cmp", "domain": "child", "attr": "source",
             "op_kind": "eq", "value": "Y"},
        ]}},
    ],
}


def ord_of(text: str) -> int:
    return datetime.date.fromisoformat(text).toordinal()


def day_of(o: int) -> str:
    return datetime.date.fromordinal(o).isoformat()


def day(n: int) -> str:
    return day_of(ord_of("2026-04-01") + n)


def merge_segments(day_state_map):
    """{ord: state} -> [(first_ord, last_ord, state)] 连续同态合并。"""
    out = []
    for o in sorted(day_state_map):
        st = day_state_map[o]
        if out and out[-1][2] == st and out[-1][1] == o - 1:
            out[-1][1] = o
        else:
            out.append([o, o, st])
    return [tuple(x) for x in out]


def make_ledger(path, days):
    """手工造一份台账：days = {date: {"token":.., "states":{eid:st}, "fp":..}}。

    区间由逐日状态独立合并得到（与 Ledger.register 的增量逻辑无关）。
    """
    ledger = Ledger(path)
    ledger.create(OLD_AST, PARENT_SCHEMA, CHILD_SCHEMA)
    per_entity = {}
    seq = 0
    for d in sorted(days, key=ord_of):
        spec = days[d]
        seq += 1
        ledger.conn.execute(
            "INSERT INTO day_source (business_date, date_ord, index_token, "
            "ast_fingerprint, registered_at, update_seq) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (d, ord_of(d), spec["token"], spec.get("fp", "fp-x"),
             "2026-04-01T00:00:00Z", seq),
        )
        for eid, st in spec.get("states", {}).items():
            per_entity.setdefault(eid, {})[ord_of(d)] = st
    for eid, dm in per_entity.items():
        for a, b, st in merge_segments(dm):
            ledger.conn.execute(
                "INSERT INTO episode_segment (entity_id, first_ord, last_ord, "
                "first_day, last_day, state) VALUES (?, ?, ?, ?, ?, ?)",
                (eid, a, b, day_of(a), day_of(b), st),
            )
    ledger.close()


# --------------------------------------------------------------------------
# 独立参考：直接枚举窗口内每一天、每一个对象算九格（不复用对照实现）
# --------------------------------------------------------------------------


def brute_force_days(left_days, right_days, src_left, src_right, start, end):
    """返回 {ord: (status, cells_key)}；cells_key 为 None 或
    (counts[9], entered[9] 或 None)。完全逐日逐对象枚举。"""
    s, e = ord_of(start), ord_of(end)
    out = {}
    prev_status = None
    for o in range(s, e + 1):
        d = day_of(o)
        lt, rt = src_left.get(d), src_right.get(d)
        if lt is not None and rt is not None:
            status = "comparable" if lt == rt else "different_source"
        elif lt is None and rt is None:
            status = "both_missing"
        elif lt is None:
            status = "left_missing"
        else:
            status = "right_missing"
        key = None
        if status == "comparable":
            lm = left_days.get(d, {})
            rm = right_days.get(d, {})
            counts = {c: 0 for c in CELLS9}
            for eid in lm.keys() & rm.keys():
                counts[(lm[eid], rm[eid])] += 1
            entered = None
            if o - 1 >= s and prev_status == "comparable":
                pl = left_days.get(day_of(o - 1), {})
                pr = right_days.get(day_of(o - 1), {})
                entered = {c: 0 for c in CELLS9}
                for eid in lm.keys() & rm.keys():
                    cell = (lm[eid], rm[eid])
                    if (eid not in pl or eid not in pr
                            or (pl[eid], pr[eid]) != cell):
                        entered[cell] += 1
            key = (tuple(counts[c] for c in CELLS9),
                   None if entered is None else
                   tuple(entered[c] for c in CELLS9))
        out[o] = (status, key)
        prev_status = status
    return out


def canon_cells(cells):
    if cells is None:
        return None
    assert [ (c["left"], c["right"]) for c in cells ] == CELLS9, \
        "可比行必须给全固定顺序的九格"
    return (tuple(c["count"] for c in cells),
            None if cells[0]["entered"] is None else
            tuple(c["entered"] for c in cells))


def decompress(rows):
    """解压变更带为 {ord: (status, cells_key)}，并校验无重无漏、递增。"""
    out = {}
    prev_end = None
    for row in rows:
        a, b = ord_of(row["first_day"]), ord_of(row["last_day"])
        assert a <= b, "段端点颠倒"
        if prev_end is not None:
            assert a == prev_end + 1, "窗口必须有衔接、无重叠"
        for o in range(a, b + 1):
            out[o] = (row["status"], canon_cells(row["cells"]))
        prev_end = b
    return out


def assert_canonical_compression(testcase, rows):
    """相邻两行不得 (status, cells) 全同（必须合并的已经合并）。"""
    for prev, nxt in zip(rows, rows[1:]):
        testcase.assertFalse(
            prev["status"] == nxt["status"]
            and canon_cells(prev["cells"]) == canon_cells(nxt["cells"]),
            f"相邻两段未合并: {prev} / {nxt}",
        )


def run_compare(left_path, right_path, start, end):
    with compare_ledgers(left_path, right_path, start, end) as run:
        rows = list(run.rows())
        return rows, run.metrics


def cell_of(row, left_state, right_state):
    for c in row["cells"]:
        if c["left"] == left_state and c["right"] == right_state:
            return c
    raise AssertionError(f"格 ({left_state},{right_state}) 不存在")


# --------------------------------------------------------------------------


class HandCraftedCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bgcompare_")
        self.left = os.path.join(self.tmp, "left.db")
        self.right = os.path.join(self.tmp, "right.db")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def check_against_brute_force(self, left_days, right_days,
                                  src_left, src_right, start, end):
        rows, _m = run_compare(self.left, self.right, start, end)
        got = decompress(rows)
        want = brute_force_days(left_days, right_days,
                                src_left, src_right, start, end)
        self.assertEqual(got, want)
        self.assertEqual(min(got), ord_of(start))
        self.assertEqual(max(got), ord_of(end))
        assert_canonical_compression(self, rows)
        return rows

    # ---- 甲乙互换：人数不变、次日两格各进入 1 人 --------------------------

    def test_swap_counts_stay_entered_counts_moves(self):
        # 甲: (false,true)->(true,false)；乙: (true,false)->(false,true)
        left_days = {
            day(1): {"甲": "false", "乙": "true"},
            day(2): {"甲": "true", "乙": "false"},
        }
        right_days = {
            day(1): {"甲": "true", "乙": "false"},
            day(2): {"甲": "false", "乙": "true"},
        }
        src = {day(1): "T1", day(2): "T2"}
        make_ledger(self.left, {d: {"token": t, "states": left_days[d]}
                                for d, t in src.items()})
        make_ledger(self.right, {d: {"token": t, "states": right_days[d]}
                                 for d, t in src.items()})
        rows = self.check_against_brute_force(
            left_days, right_days, src, src, day(1), day(2))
        self.assertEqual(len(rows), 2)
        r1, r2 = rows
        # 首日：窗口起点，entered 不可计算
        self.assertEqual((r1["first_day"], r1["last_day"]), (day(1), day(1)))
        self.assertEqual(cell_of(r1, "false", "true")["count"], 1)
        self.assertEqual(cell_of(r1, "true", "false")["count"], 1)
        self.assertIsNone(cell_of(r1, "false", "true")["entered"])
        # 次日：两格人数仍各为 1，但两格各进入 1 人（甲、乙互换）
        self.assertEqual((r2["first_day"], r2["last_day"]), (day(2), day(2)))
        self.assertEqual(cell_of(r2, "false", "true")["count"], 1)
        self.assertEqual(cell_of(r2, "true", "false")["count"], 1)
        self.assertEqual(cell_of(r2, "false", "true")["entered"], 1)
        self.assertEqual(cell_of(r2, "true", "false")["entered"], 1)
        self.assertEqual(cell_of(r2, "true", "true")["entered"], 0)

    # ---- 中间日不同代号：该天无结果、次日 entered 不可计算 -----------------

    def test_different_source_day_blocks_and_nulls_next_entered(self):
        # 4 天内容完全相同（两对象稳定），第 2 天两边代号不同
        states = {"A": "true", "B": "false"}
        pair = {"A": "unknown", "B": "true"}
        left_days = {day(i): dict(states) for i in (0, 1, 2, 3)}
        right_days = {day(i): dict(pair) for i in (0, 1, 2, 3)}
        src_l = {day(0): "T0", day(1): "TL", day(2): "T2", day(3): "T3"}
        src_r = {day(0): "T0", day(1): "TR", day(2): "T2", day(3): "T3"}
        make_ledger(self.left, {d: {"token": t, "states": left_days[d]}
                                for d, t in src_l.items()})
        make_ledger(self.right, {d: {"token": t, "states": right_days[d]}
                                 for d, t in src_r.items()})
        rows = self.check_against_brute_force(
            left_days, right_days, src_l, src_r, day(0), day(3))
        self.assertEqual([r["status"] for r in rows],
                         ["comparable", "different_source",
                          "comparable", "comparable"])
        # 不同代号当天：无比较结果
        self.assertIsNone(rows[1]["cells"])
        self.assertEqual((rows[1]["first_day"], rows[1]["last_day"]),
                         (day(1), day(1)))
        # 紧随其后的可比日：各格人数没变，但 entered 不可计算（null 非零）
        after = rows[2]
        self.assertEqual((after["first_day"], after["last_day"]),
                         (day(2), day(2)))
        self.assertEqual(cell_of(after, "true", "unknown")["count"], 1)
        self.assertEqual(cell_of(after, "false", "true")["count"], 1)
        for c in after["cells"]:
            self.assertIsNone(c["entered"])
        # 再往后一天：entered 恢复可计算，全员未动 → 全 0
        nxt = rows[3]
        for c in nxt["cells"]:
            self.assertEqual(c["entered"], 0)

    # ---- 缺报状态与整窗 both_missing -------------------------------------

    def test_missing_statuses_and_whole_window_both_missing(self):
        src_l = {day(0): "T0", day(2): "T2"}
        src_r = {day(1): "T1", day(2): "TX"}  # day2 代号不同
        make_ledger(self.left, {d: {"token": t, "states": {"A": "true"}}
                                for d, t in src_l.items()})
        make_ledger(self.right, {d: {"token": t, "states": {"A": "true"}}
                                 for d, t in src_r.items()})
        rows, _ = run_compare(self.left, self.right, day(0), day(4))
        self.assertEqual(
            [(r["first_day"], r["last_day"], r["status"]) for r in rows],
            [(day(0), day(0), "right_missing"),
             (day(1), day(1), "left_missing"),
             (day(2), day(2), "different_source"),
             (day(3), day(4), "both_missing")])
        for r in rows:
            self.assertIsNone(r["cells"])

        # 整窗无任何登记：恰好一个 both_missing 段
        rows2, _ = run_compare(self.left, self.right, day(10), day(13))
        self.assertEqual(len(rows2), 1)
        self.assertEqual((rows2[0]["first_day"], rows2[0]["last_day"],
                          rows2[0]["status"], rows2[0]["cells"]),
                         (day(10), day(13), "both_missing", None))

    # ---- 只认当天代号；不看规则指纹 ---------------------------------------

    def test_same_token_on_other_date_does_not_count(self):
        make_ledger(self.left, {day(0): {"token": "T", "states": {"A": "true"}}})
        make_ledger(self.right, {day(1): {"token": "T", "states": {"A": "true"}}})
        rows, _ = run_compare(self.left, self.right, day(0), day(1))
        self.assertEqual([r["status"] for r in rows],
                         ["right_missing", "left_missing"])

    def test_fingerprint_difference_does_not_block(self):
        make_ledger(self.left, {day(0): {"token": "T", "fp": "old-rule",
                                         "states": {"A": "true"}}})
        make_ledger(self.right, {day(0): {"token": "T", "fp": "new-rule",
                                          "states": {"A": "false"}}})
        rows, _ = run_compare(self.left, self.right, day(0), day(0))
        self.assertEqual(rows[0]["status"], "comparable")
        self.assertEqual(cell_of(rows[0], "true", "false")["count"], 1)

    # ---- 缺席不算 unknown；对象出入计入 entered ----------------------------

    def test_absent_is_not_unknown_and_churn_counts_as_entered(self):
        left_days = {
            day(0): {"A": "true", "B": "false", "C": "unknown"},
            day(1): {"A": "true", "C": "unknown", "D": "false"},
        }
        right_days = {
            day(0): {"A": "true", "C": "false"},
            day(1): {"A": "false", "C": "false", "D": "unknown"},
        }
        src = {day(0): "T0", day(1): "T1"}
        make_ledger(self.left, {d: {"token": t, "states": left_days[d]}
                                for d, t in src.items()})
        make_ledger(self.right, {d: {"token": t, "states": right_days[d]}
                                 for d, t in src.items()})
        rows = self.check_against_brute_force(
            left_days, right_days, src, src, day(0), day(1))
        r0, r1 = rows
        # day0：B 只在左边 → 不进任何格（绝不算 unknown）；总人数 = 2
        self.assertEqual(sum(c["count"] for c in r0["cells"]), 2)
        self.assertEqual(cell_of(r0, "true", "true")["count"], 1)
        self.assertEqual(cell_of(r0, "unknown", "false")["count"], 1)
        # day1：A 变态 (true,true)->(true,false)；D 新进入对照集
        self.assertEqual(cell_of(r1, "true", "false")["count"], 1)
        self.assertEqual(cell_of(r1, "true", "false")["entered"], 1)
        self.assertEqual(cell_of(r1, "false", "unknown")["count"], 1)
        self.assertEqual(cell_of(r1, "false", "unknown")["entered"], 1)
        self.assertEqual(cell_of(r1, "unknown", "false")["entered"], 0)

    # ---- 窗口裁剪 ----------------------------------------------------------

    def test_window_clipping(self):
        # 历史 0..9 全部可比且稳定；窗口裁 3..6
        left_days = {day(i): {"A": "true", "B": "false"} for i in range(10)}
        right_days = {day(i): {"A": "true", "B": "unknown"} for i in range(10)}
        src = {day(i): f"T{i}" for i in range(10)}
        make_ledger(self.left, {d: {"token": t, "states": left_days[d]}
                                for d, t in src.items()})
        make_ledger(self.right, {d: {"token": t, "states": right_days[d]}
                                 for d, t in src.items()})
        rows = self.check_against_brute_force(
            left_days, right_days, src, src, day(3), day(6))
        self.assertEqual(len(rows), 2)
        # 窗口首日 entered 为 null（前一自然日虽可比，但不在窗口内）
        self.assertEqual((rows[0]["first_day"], rows[0]["last_day"]),
                         (day(3), day(3)))
        for c in rows[0]["cells"]:
            self.assertIsNone(c["entered"])
        # 其余三天合并为一段，entered 全 0
        self.assertEqual((rows[1]["first_day"], rows[1]["last_day"]),
                         (day(4), day(6)))
        for c in rows[1]["cells"]:
            self.assertEqual(c["entered"], 0)

    # ---- 空批次同源日可比 ---------------------------------------------------

    def test_empty_batch_same_source_is_comparable(self):
        # 两边登记同一代号，但当天对照对象集为空（两边对象不相交）
        make_ledger(self.left, {day(0): {"token": "T", "states": {"A": "true"}},
                                day(1): {"token": "U", "states": {"A": "true"}}})
        make_ledger(self.right, {day(0): {"token": "T", "states": {"B": "false"}},
                                 day(1): {"token": "U", "states": {"B": "false"}}})
        rows, _ = run_compare(self.left, self.right, day(0), day(1))
        self.assertEqual([r["status"] for r in rows],
                         ["comparable", "comparable"])
        self.assertEqual(sum(c["count"] for c in rows[0]["cells"]), 0)
        self.assertIsNone(rows[0]["cells"][0]["entered"])
        self.assertEqual(sum(c["count"] for c in rows[1]["cells"]), 0)
        self.assertEqual(rows[1]["cells"][0]["entered"], 0)

    # ---- 必须合并：相邻同态（含非零 entered 相同）合成一段 ------------------

    def test_adjacent_identical_days_must_merge(self):
        # 三天全员稳定 → [首日 entered=null] + [后两天 entered=0 合并]
        left_days = {day(i): {"A": "true"} for i in range(3)}
        right_days = {day(i): {"A": "false"} for i in range(3)}
        src = {day(i): f"T{i}" for i in range(3)}
        make_ledger(self.left, {d: {"token": t, "states": left_days[d]}
                                for d, t in src.items()})
        make_ledger(self.right, {d: {"token": t, "states": right_days[d]}
                                 for d, t in src.items()})
        rows, _ = run_compare(self.left, self.right, day(0), day(2))
        self.assertEqual([(r["first_day"], r["last_day"]) for r in rows],
                         [(day(0), day(0)), (day(1), day(2))])

    def test_consecutive_equal_nonzero_entered_merges(self):
        # 第 1、2 天各有一对对象互换格子：两格人数不变、entered 同为 1
        left_days = {
            day(0): {"A": "true", "B": "false", "C": "true", "D": "false"},
            day(1): {"A": "false", "B": "true", "C": "true", "D": "false"},
            day(2): {"A": "false", "B": "true", "C": "false", "D": "true"},
        }
        right_days = {
            day(0): {"A": "false", "B": "true", "C": "false", "D": "true"},
            day(1): {"A": "true", "B": "false", "C": "false", "D": "true"},
            day(2): {"A": "true", "B": "false", "C": "true", "D": "false"},
        }
        src = {day(i): f"T{i}" for i in range(3)}
        make_ledger(self.left, {d: {"token": t, "states": left_days[d]}
                                for d, t in src.items()})
        make_ledger(self.right, {d: {"token": t, "states": right_days[d]}
                                 for d, t in src.items()})
        rows = self.check_against_brute_force(
            left_days, right_days, src, src, day(0), day(2))
        # day1 与 day2 的 (counts, entered) 完全相同 → 必须合并成一段
        self.assertEqual([(r["first_day"], r["last_day"]) for r in rows],
                         [(day(0), day(0)), (day(1), day(2))])
        seg = rows[1]
        self.assertEqual(cell_of(seg, "true", "false")["count"], 2)
        self.assertEqual(cell_of(seg, "false", "true")["count"], 2)
        self.assertEqual(cell_of(seg, "true", "false")["entered"], 1)
        self.assertEqual(cell_of(seg, "false", "true")["entered"], 1)

    # ---- 九格形状 ------------------------------------------------------------

    def test_nine_cells_shape_and_order(self):
        make_ledger(self.left, {day(0): {"token": "T", "states": {"A": "unknown"}}})
        make_ledger(self.right, {day(0): {"token": "T", "states": {"A": "unknown"}}})
        rows, _ = run_compare(self.left, self.right, day(0), day(0))
        cells = rows[0]["cells"]
        self.assertEqual(len(cells), 9)
        self.assertEqual([(c["left"], c["right"]) for c in cells], CELLS9)
        self.assertEqual(sum(c["count"] for c in cells), 1)
        self.assertEqual(cell_of(rows[0], "unknown", "unknown")["count"], 1)

    # ---- 只读与快照 ----------------------------------------------------------

    def test_ledger_files_untouched_by_compare(self):
        left_days = {day(i): {"A": "true", "B": "false"} for i in range(4)}
        right_days = {day(i): {"A": "true", "B": "unknown"} for i in range(4)}
        src = {day(i): f"T{i}" for i in range(4)}
        make_ledger(self.left, {d: {"token": t, "states": left_days[d]}
                                for d, t in src.items()})
        make_ledger(self.right, {d: {"token": t, "states": right_days[d]}
                                 for d, t in src.items()})

        def dir_digest():
            out = {}
            for name in sorted(os.listdir(self.tmp)):
                with open(os.path.join(self.tmp, name), "rb") as fh:
                    out[name] = hashlib.sha256(fh.read()).hexdigest()
            return out

        before = dir_digest()
        rows, _ = run_compare(self.left, self.right, day(0), day(3))
        self.assertTrue(rows)
        self.assertEqual(dir_digest(), before)  # 无内容变化、无边车残留

    def test_snapshot_is_stable_against_concurrent_writer(self):
        left_days = {day(i): {"A": "true"} for i in range(3)}
        right_days = {day(i): {"A": "false"} for i in range(3)}
        src = {day(i): f"T{i}" for i in range(3)}
        make_ledger(self.left, {d: {"token": t, "states": left_days[d]}
                                for d, t in src.items()})
        make_ledger(self.right, {d: {"token": t, "states": right_days[d]}
                                 for d, t in src.items()})
        # 写者在对照运行期间把 day(1) 换成另一个代号（更正重登记）
        writer = Ledger(self.left)
        try:
            run = compare_ledgers(self.left, self.right, day(0), day(2))
            try:
                writer.conn.execute("BEGIN IMMEDIATE")
                writer.conn.execute(
                    "UPDATE day_source SET index_token = 'CORRECTED' "
                    "WHERE business_date = ?", (day(1),))
                writer.conn.execute("COMMIT")
                rows = list(run.rows())  # 快照在 writer 提交前已固定
            finally:
                run.close()
        finally:
            writer.close()
        # 旧快照：day(1) 仍然可比（三天同态稳定，压缩为 [d0] + [d1..d2]）
        self.assertEqual([r["status"] for r in rows],
                         ["comparable", "comparable"])
        self.assertEqual((rows[1]["first_day"], rows[1]["last_day"]),
                         (day(1), day(2)))
        # 重新对照则看到更正后的世界：day(1) 变为 different_source
        rows2, _ = run_compare(self.left, self.right, day(0), day(2))
        self.assertEqual([r["status"] for r in rows2],
                         ["comparable", "different_source", "comparable"])

    # ---- 参数校验 ------------------------------------------------------------

    def test_invalid_arguments_rejected(self):
        make_ledger(self.left, {day(0): {"token": "T", "states": {"A": "true"}}})
        make_ledger(self.right, {day(0): {"token": "T", "states": {"A": "true"}}})
        with self.assertRaises(LedgerError):
            compare_ledgers(self.left, self.right, day(2), day(1))
        with self.assertRaises(LedgerError):
            compare_ledgers(self.left, self.right, "2026-4-01", day(1))
        with self.assertRaises(LedgerError):
            compare_ledgers(self.left, self.right, day(0), "2026-04-32")
        with self.assertRaises(LedgerError):
            compare_ledgers(self.left,
                            os.path.join(self.tmp, "missing.db"),
                            day(0), day(1))


# --------------------------------------------------------------------------
# 全管线：Importer + Ledger.register（两套规则各记一份台账）+ 直接枚举核对
# --------------------------------------------------------------------------

# 子行组合 → (老规则状态, 新规则状态)
KIND_CHILDREN = {
    "tf": [(10, "X")],                      # (true, false)
    "ft": [(10, "Y")],                      # (false, true)
    "TT": [(10, "X"), (10, "Y")],           # (true, true)
    "ff": [(10, "Z")],                      # (false, false)
    "tu": [(10, "X"), (None, "Y")],         # (true, unknown)
    "fu": [(None, "Y")],                    # (false, unknown)
    "uf": [(None, "X")],                    # (unknown, false)
    "ut": [(None, "X"), (10, "Y")],         # (unknown, true)
    "uu": [(None, "X"), (None, "Y")],       # (unknown, unknown)
}


def eval_state(children, source_value):
    """独立算单对象状态：zone==N 且存在 (temp<25 且 source==source_value)。"""
    best = "false"
    for temp, source in children:
        if source == source_value:
            best = "unknown" if temp is None else "true"
            if best == "true":
                return "true"
    return best


class FullPipelineCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bgcompare_e2e_")
        self.left = os.path.join(self.tmp, "old.db")
        self.right = os.path.join(self.tmp, "new.db")
        self._n = 0

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def build_index(self, present):
        """present: {eid: kind|None}；kind 见 KIND_CHILDREN，None 表示缺席。"""
        self._n += 1
        base = os.path.join(self.tmp, f"idx{self._n:02d}")
        os.makedirs(base)
        rows_p, rows_c = [], []
        for i, (eid, kind) in enumerate(present.items()):
            rows_p.append({"entity_id": eid, "zone": "N", "level": i})
            for j, (temp, source) in enumerate(
                    KIND_CHILDREN.get(kind, [])):
                rows_c.append({"child_id": f"{eid}-{j}", "entity_id": eid,
                               "temp": temp, "source": source})
        pp, cp = os.path.join(base, "p.jsonl"), os.path.join(base, "c.jsonl")
        with open(pp, "w", encoding="utf-8") as fh:
            for r in rows_p:
                fh.write(json.dumps(r) + "\n")
        with open(cp, "w", encoding="utf-8") as fh:
            for r in rows_c:
                fh.write(json.dumps(r) + "\n")
        idx = os.path.join(base, "index")
        Importer(idx, PARENT_SCHEMA, CHILD_SCHEMA).run(pp, cp)
        return idx

    def test_full_pipeline_with_brute_force(self):
        # 每天的内容：{eid: kind|None}
        content = {
            day(0): {"甲": "ft", "乙": "tf", "丙": "TT", "丁": "uu"},
            day(1): {"甲": "ft", "乙": "tf", "丙": "TT", "丁": "uu"},
            day(2): {"甲": "tf", "乙": "ft", "丙": "TT", "丁": "uu", "戊": "uf"},
            day(3): {"甲": "tf", "乙": "ft", "丙": "TT", "丁": "uu", "戊": "uf"},
            day(4): {"甲": "tf", "乙": "ft", "丙": "TT", "丁": "uu", "戊": "uf"},
            day(5): {"甲": "tf", "乙": "ft", "丙": "TT"},
            day(7): {"甲": "tf", "乙": "ft", "丙": "TT", "己": "ut"},
            day(8): {"甲": "tf", "乙": "ft", "丙": "TT", "己": "ut"},
        }
        # 各日索引：day(3) 左右各用一份（同内容、不同代号）；day(5) 只有右边；
        # day(6) 两边都不登记（both_missing）
        idx = {d: self.build_index(c) for d, c in content.items()}
        idx_alt_right = self.build_index(content[day(3)])  # 同内容新代号
        idx_right_only = self.build_index(content[day(5)])

        left_days, right_days = {}, {}
        src_l, src_r = {}, {}
        with Ledger(self.left) as ll, Ledger(self.right) as rr:
            ll.create(OLD_AST, PARENT_SCHEMA, CHILD_SCHEMA)
            rr.create(NEW_AST, PARENT_SCHEMA, CHILD_SCHEMA)
            for d, c in content.items():
                old_map = {e: eval_state(KIND_CHILDREN.get(k, []), "X")
                           for e, k in c.items() if k is not None}
                new_map = {e: eval_state(KIND_CHILDREN.get(k, []), "Y")
                           for e, k in c.items() if k is not None}
                if d == day(5):
                    src_r[d] = rr.register(d, idx_right_only).index_token
                    right_days[d] = new_map
                    continue
                if d == day(3):
                    src_l[d] = ll.register(d, idx[d]).index_token
                    src_r[d] = rr.register(d, idx_alt_right).index_token
                else:
                    src_l[d] = ll.register(d, idx[d]).index_token
                    src_r[d] = rr.register(d, idx[d]).index_token
                left_days[d] = old_map
                right_days[d] = new_map
        # day(6) 两边都缺报 → both_missing；窗口裁掉 day(0) 与 day(8)
        start, end = day(1), day(7)
        rows, metrics = run_compare(self.left, self.right, start, end)
        got = decompress(rows)
        want = brute_force_days(left_days, right_days, src_l, src_r,
                                start, end)
        self.assertEqual(got, want)
        assert_canonical_compression(self, rows)

        def row_containing(d):
            for r in rows:
                if ord_of(r["first_day"]) <= ord_of(d) <= ord_of(r["last_day"]):
                    return r
            raise AssertionError(d)

        # 关键点位：互换次日两格各进入 1 人
        d2 = row_containing(day(2))
        self.assertEqual(d2["first_day"], day(2))  # 当天 entered 变化，独立成段
        self.assertEqual(cell_of(d2, "true", "false")["entered"], 1)
        self.assertEqual(cell_of(d2, "false", "true")["entered"], 1)
        self.assertEqual(cell_of(d2, "unknown", "false")["entered"], 1)  # 戊
        # day(3) 不同代号 → 无结果；day(4) entered 不可计算
        self.assertEqual(got[ord_of(day(3))][0], "different_source")
        self.assertEqual(got[ord_of(day(5))][0], "left_missing")
        self.assertEqual(got[ord_of(day(6))][0], "both_missing")
        d4 = row_containing(day(4))
        self.assertEqual(d4["first_day"], day(4))
        for c in d4["cells"]:
            self.assertIsNone(c["entered"])
        # day(7) 前一日 both_missing → entered 同样不可计算
        d7 = row_containing(day(7))
        self.assertEqual(d7["first_day"], day(7))
        for c in d7["cells"]:
            self.assertIsNone(c["entered"])
        # 指标：窗口内 4 个可比日 + 1 不同代号 + 1 左缺 + 1 双缺
        self.assertEqual(metrics["status_day_counts"]["comparable"], 4)
        self.assertEqual(metrics["status_day_counts"]["different_source"], 1)
        self.assertEqual(metrics["status_day_counts"]["left_missing"], 1)
        self.assertEqual(metrics["status_day_counts"]["both_missing"], 1)

    def test_cli_jsonl_and_metrics(self):
        content = {day(0): {"甲": "ft", "乙": "tf"},
                   day(1): {"甲": "tf", "乙": "ft"}}
        idx = {d: self.build_index(c) for d, c in content.items()}
        with Ledger(self.left) as ll, Ledger(self.right) as rr:
            ll.create(OLD_AST, PARENT_SCHEMA, CHILD_SCHEMA)
            rr.create(NEW_AST, PARENT_SCHEMA, CHILD_SCHEMA)
            for d, i in idx.items():
                ll.register(d, i)
                rr.register(d, i)
        out_path = os.path.join(self.tmp, "band.jsonl")
        err = io.StringIO()
        with redirect_stderr(err):
            rc = cli.main([
                "ledger-compare",
                "--left", self.left, "--right", self.right,
                "--start", day(0), "--end", day(1),
                "--out", out_path,
            ])
        self.assertEqual(rc, 0)
        with open(out_path, encoding="utf-8") as fh:
            lines = [json.loads(x) for x in fh if x.strip()]
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0]["status"], "comparable")
        self.assertEqual(len(lines[0]["cells"]), 9)
        metrics = json.loads(err.getvalue())["metrics"]
        self.assertEqual(metrics["window"]["days"], 2)
        self.assertEqual(metrics["output_rows"], 2)
        self.assertEqual(metrics["entities_compared"], 2)
        # 台账文件未被修改、无边车残留
        self.assertEqual(sorted(os.listdir(self.tmp)),
                         sorted(n for n in os.listdir(self.tmp)
                                if not n.endswith(("-wal", "-shm"))))


if __name__ == "__main__":
    unittest.main()
