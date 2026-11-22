"""历史状态台账的端到端测试。

覆盖题目要求的全部验收点：

* 从空库创建、按业务日期登记、按自然日合并；
* 独立核对（不复用台账更新逻辑）整张区间表与每日来源登记；
* 同一对象在不同日报改变行序；false / unknown / 缺报 / 已登记但不含该
  对象的日期并存；
* 乱序补报合并两侧、历史更正拆段、重送幂等、只换来源不改判定；
* 移开已用索引后再更正；
* 写事务提交前中断（注入异常），重开核对旧登记与旧区间，再验证重送成功；
* 并发读在写事务中只看到完整旧版或完整新版；
* 两份可生成历史对照（同日更正、不同长度无关历史），以实际读数证明
  更新没有扫描旧历史。

运行：``PYTHONPATH=/workspace python3 -m unittest tests.test_ledger -v``
"""

from __future__ import annotations

import datetime
import json
import os
import shutil
import sqlite3
import tempfile
import threading
import unittest

from bitgroup.importer import Importer
from bitgroup.ledger import Ledger, LedgerError

PARENT_SCHEMA = {"zone": "text", "level": "int64"}
CHILD_SCHEMA = {"temp": "int64", "source": "text"}

# 固定查询规则：zone == "N" 且存在 (temp < 25 且 source == "X") 的子行。
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


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def day(n: int) -> str:
    """以 2026-03-01 为基准偏移 n 天的业务日期文本。"""
    return (datetime.date(2026, 3, 1) + datetime.timedelta(days=n)).isoformat()


def ord_of(text: str) -> int:
    return datetime.date.fromisoformat(text).toordinal()


class LedgerCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bgledger_")
        self.db = os.path.join(self.tmp, "ledger.db")
        self._built = 0

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ---- 夹具构建 ------------------------------------------------------

    def build_index(self, present):
        """present: entity_id -> 期望状态或 None(对象不在该日报里)。

        true   : 有 temp<25 且 source=X 的子行
        false  : 有子行但都不满足；或对象在但无合格子行
        unknown: 无 true 但有 unknown 子行
        """
        rows_p, rows_c = [], []
        for i, (eid, kind) in enumerate(present.items()):
            rows_p.append({"entity_id": eid, "zone": "N", "level": i})
            if kind is None:
                continue
            if kind == "true":
                rows_c.append({"child_id": f"{eid}-t", "entity_id": eid,
                               "temp": 10, "source": "X"})
            elif kind == "false":
                rows_c.append({"child_id": f"{eid}-f", "entity_id": eid,
                               "temp": 10, "source": "Y"})
            elif kind == "unknown":
                rows_c.append({"child_id": f"{eid}-u", "entity_id": eid,
                               "temp": None, "source": "X"})
            else:
                raise AssertionError(kind)
        self._built += 1
        base = os.path.join(self.tmp, f"idx{self._built:04d}")
        os.makedirs(base)
        pp, cp = os.path.join(base, "p.jsonl"), os.path.join(base, "c.jsonl")
        write_jsonl(pp, rows_p)
        write_jsonl(cp, rows_c)
        idx = os.path.join(base, "index")
        Importer(idx, PARENT_SCHEMA, CHILD_SCHEMA).run(pp, cp)
        return idx

    def build_index_ordered(self, ordered_pairs):
        """按给定行序（列表）构造，用于让同一对象在不同日报改变行序。"""
        return self.build_index(dict(ordered_pairs))

    # ---- 独立核对：直接读原始 JSONL 逐日判定，再按自然日合并 ----------

    def independent_episodes(self, day_pairs):
        """day_pairs: [(business_date, [(entity_id, kind), ...]), ...]

        完全不复用台账的更新逻辑：直接在 Python 里逐日算三态、构造
        (date_ord -> {eid: state}) 映射，再按自然日贪心合并同态相邻段。
        """
        states = {}
        for date_text, pairs in day_pairs:
            o = ord_of(date_text)
            day_map = {}
            for eid, kind in pairs:
                if kind is None:
                    continue  # 当日已登记但无此对象：不产出
                day_map[eid] = kind
            states[o] = day_map
        per_entity = {}
        for o, dm in states.items():
            for eid, st in dm.items():
                per_entity.setdefault(eid, {})[o] = st
        out = set()
        for eid, dm in per_entity.items():
            for o in sorted(dm):
                st = dm[o]
                if dm.get(o - 1) == st:
                    continue  # 上一天同态，已并入前段
                j = o
                while dm.get(j + 1) == st:
                    j += 1
                out.add((eid, o, j, st))
        return out

    def ledger_episodes_raw(self, ledger):
        """直接使用 SQLite 视图读取整张区间表（不经 Ledger.episodes）。"""
        return {
            (r[0], ord_of(r[1]), ord_of(r[2]), r[3])
            for r in ledger.conn.execute(
                "SELECT entity_id, first_day, last_day, state FROM episode"
            )
        }

    def ledger_source_map(self, ledger):
        return {
            r[0]: r[1]
            for r in ledger.conn.execute(
                "SELECT business_date, index_token FROM daily_source"
            )
        }

    def assert_table_matches(self, ledger, day_pairs):
        want = self.independent_episodes(day_pairs)
        got = self.ledger_episodes_raw(ledger)
        self.assertEqual(got, want)

    def register(self, ledger, date_text, present):
        return ledger.register(date_text, self.build_index(present))

    # ==================================================================
    # 1. 从空库运行 + 手算小夹具
    # ==================================================================

    def test_create_then_hand_fixture_interval_merge(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            self.assertTrue(L.exists)
            # 周一 A=true, B=false, C=unknown
            L.register(day(0), self.build_index(
                {"A": "true", "B": "false", "C": "unknown"}))
            # 周二 A=true, B=false, C 不在该日报；D=true
            L.register(day(1), self.build_index(
                {"A": "true", "B": "false", "D": "true"}))
            # 周三 A=true, B=unknown, D 不在
            L.register(day(2), self.build_index(
                {"A": "true", "B": "unknown", "C": "true"}))
            eps = {(e["entity_id"], e["first_day"], e["last_day"], e["state"])
                   for e in L.episodes()}
            self.assertEqual(eps, {
                # A 三天都 true，合并为一段
                ("A", day(0), day(2), "true"),
                # B: false 两天，周三 unknown 断开
                ("B", day(0), day(1), "false"),
                ("B", day(2), day(2), "unknown"),
                # C: 周一 unknown，周二缺该对象（断），周三 true
                ("C", day(0), day(0), "unknown"),
                ("C", day(2), day(2), "true"),
                # D: 仅周二
                ("D", day(1), day(1), "true"),
            })
            # 独立核对整张区间表
            self.assert_table_matches(L, [
                (day(0), [("A", "true"), ("B", "false"), ("C", "unknown")]),
                (day(1), [("A", "true"), ("B", "false"), ("D", "true")]),
                (day(2), [("A", "true"), ("B", "unknown"), ("C", "true")]),
            ])

    # ==================================================================
    # 2. 行序变化 + 缺报 + 已登记但不含该对象，false/unknown 并存
    # ==================================================================

    def test_row_order_and_missing_objects(self):
        # 同一批对象，在不同日报里行序不同
        orders = [
            [("A", "true"), ("B", "false"), ("C", "unknown")],
            [("C", "unknown"), ("A", "true"), ("B", "false")],
            [("B", "false"), ("C", "unknown"), ("A", "true")],
        ]
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            L.register(day(0), self.build_index_ordered(orders[0]))
            L.register(day(1), self.build_index_ordered(orders[1]))
            # 第 3 天缺报（不登记）；第 4 天行序再次不同
            L.register(day(3), self.build_index_ordered(orders[2]))
            eps = {(e["entity_id"], e["first_day"], e["last_day"], e["state"])
                   for e in L.episodes()}
            # 缺报的第 2 天（day(1) 与 day(3) 之间隔了 day(2)）切断连续性
            self.assertEqual(eps, {
                ("A", day(0), day(1), "true"),
                ("B", day(0), day(1), "false"),
                ("C", day(0), day(1), "unknown"),
                ("A", day(3), day(3), "true"),
                ("B", day(3), day(3), "false"),
                ("C", day(3), day(3), "unknown"),
            })
            # 来源登记只含已登记的三天
            src = self.ledger_source_map(L)
            self.assertEqual(sorted(src), [day(0), day(1), day(3)])
            # 区分“缺报”与“已登记但无该对象”：
            # day(3) 的日报里没有 D；查询 D 在 day(3) 逐日状态应为空
            self.assertEqual(L.object_days("D", start=day(3), end=day(3)), [])
            self.assertNotIn(day(2), src)  # 缺报：根本没有来源登记

    def test_registered_day_without_object_cuts_continuity(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            L.register(day(0), self.build_index({"A": "true", "B": "true"}))
            # day(1) 已登记，但日报里没有 A
            L.register(day(1), self.build_index({"B": "true"}))
            L.register(day(2), self.build_index({"A": "true", "B": "true"}))
            eps = {(e["entity_id"], e["first_day"], e["last_day"], e["state"])
                   for e in L.episodes()}
            self.assertEqual(eps, {
                ("A", day(0), day(0), "true"),
                ("A", day(2), day(2), "true"),
                ("B", day(0), day(2), "true"),
            })
            # 该日已登记：来源存在，但 day_state 无 A
            self.assertEqual(len(L.daily_sources(business_date=day(1))), 1)
            self.assertEqual(
                [r["business_date"] for r in L.object_days("A")],
                [day(0), day(2)],
            )

    # ==================================================================
    # 3. 乱序补报：同为 true 的中间日补齐后合并两段
    # ==================================================================

    def test_out_of_order_backfill_merges_two_sides(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            L.register(day(0), self.build_index({"A": "true", "B": "true"}))
            L.register(day(2), self.build_index({"A": "true", "B": "true"}))
            # 周一周三 true，周二缺报 -> A/B 各两段
            eps = {(e["entity_id"], e["first_day"], e["last_day"], e["state"])
                   for e in L.episodes()}
            self.assertEqual(eps, {
                ("A", day(0), day(0), "true"), ("A", day(2), day(2), "true"),
                ("B", day(0), day(0), "true"), ("B", day(2), day(2), "true"),
            })
            # 补进同为 true 的周二（乱序）
            L.register(day(1), self.build_index({"A": "true", "B": "true"}))
            eps = {(e["entity_id"], e["first_day"], e["last_day"], e["state"])
                   for e in L.episodes()}
            self.assertEqual(eps, {
                ("A", day(0), day(2), "true"),
                ("B", day(0), day(2), "true"),
            })
            self.assert_table_matches(L, [
                (day(0), [("A", "true"), ("B", "true")]),
                (day(1), [("A", "true"), ("B", "true")]),
                (day(2), [("A", "true"), ("B", "true")]),
            ])

    def test_out_of_order_backfill_with_state_change_does_not_merge(self):
        # 补齐的中间日若状态不同，则不应合并
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            L.register(day(0), self.build_index({"A": "true"}))
            L.register(day(2), self.build_index({"A": "true"}))
            L.register(day(1), self.build_index({"A": "false"}))
            eps = {(e["entity_id"], e["first_day"], e["last_day"], e["state"])
                   for e in L.episodes()}
            self.assertEqual(eps, {
                ("A", day(0), day(0), "true"),
                ("A", day(1), day(1), "false"),
                ("A", day(2), day(2), "true"),
            })

    # ==================================================================
    # 4. 历史更正：更正移除中间对象后拆回两段
    # ==================================================================

    def test_historical_correction_splits_and_resplits(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            for i in range(3):
                L.register(day(i), self.build_index({"A": "true", "B": "true"}))
            self.assertEqual(
                {e["entity_id"]: (e["first_day"], e["last_day"])
                 for e in L.episodes()},
                {"A": (day(0), day(2)), "B": (day(0), day(2))},
            )
            # 更正周二并移除了 A（B 仍在）
            L.register(day(1), self.build_index({"B": "true"}))
            eps = {(e["entity_id"], e["first_day"], e["last_day"], e["state"])
                   for e in L.episodes()}
            self.assertEqual(eps, {
                ("A", day(0), day(0), "true"),
                ("A", day(2), day(2), "true"),
                ("B", day(0), day(2), "true"),
            })
            self.assert_table_matches(L, [
                (day(0), [("A", "true"), ("B", "true")]),
                (day(1), [("B", "true")]),
                (day(2), [("A", "true"), ("B", "true")]),
            ])
            # 再更正周二，把 A 以相同状态放回，两段重新合并
            L.register(day(1), self.build_index({"A": "true", "B": "true"}))
            self.assertEqual(
                {e["entity_id"]: (e["first_day"], e["last_day"])
                 for e in L.episodes()},
                {"A": (day(0), day(2)), "B": (day(0), day(2))},
            )

    def test_correction_changes_state_inside_segment(self):
        # 中间日状态改变：一段 true 被切成 true/false/true
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            for i in range(5):
                L.register(day(i), self.build_index({"A": "true"}))
            self.assertEqual(len(L.episodes()), 1)
            L.register(day(2), self.build_index({"A": "unknown"}))
            eps = {(e["first_day"], e["last_day"], e["state"])
                   for e in L.episodes()}
            self.assertEqual(eps, {
                (day(0), day(1), "true"),
                (day(2), day(2), "unknown"),
                (day(3), day(4), "true"),
            })

    # ==================================================================
    # 5. 重送幂等
    # ==================================================================

    def test_resend_same_index_token_is_idempotent(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            idx = self.build_index({"A": "true", "B": "false"})
            r1 = L.register(day(0), idx)
            eps1 = self.ledger_episodes_raw(L)
            src1 = self.ledger_source_map(L)
            # 同一天再次登记同一索引代号：来源与区间不改变
            r2 = L.register(day(0), idx)
            self.assertTrue(r2.replaced)
            self.assertEqual(r2.changed_entities, 0)
            self.assertEqual(r2.episode_adjustments, 0)
            self.assertEqual(self.ledger_episodes_raw(L), eps1)
            self.assertEqual(self.ledger_source_map(L), src1)
            self.assertEqual(r2.index_token, r1.index_token)

    # ==================================================================
    # 6. 只换来源、不改判定：必须更新该日来源登记
    # ==================================================================

    def test_new_token_same_verdict_updates_source_only(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            idx_a = self.build_index({"A": "true", "B": "false", "C": "unknown"})
            ra = L.register(day(0), idx_a)
            eps_before = self.ledger_episodes_raw(L)
            # 不同索引代号，但三态判定结果完全相同
            idx_b = self.build_index({"C": "unknown", "A": "true", "B": "false"})
            rb = L.register(day(0), idx_b)
            self.assertNotEqual(ra.index_token, rb.index_token)
            # 判定没变：零调整、区间不变
            self.assertEqual(rb.changed_entities, 0)
            self.assertEqual(rb.episode_adjustments, 0)
            self.assertEqual(self.ledger_episodes_raw(L), eps_before)
            # 但该日来源登记必须换成新代号
            src = self.ledger_source_map(L)
            self.assertEqual(src[day(0)], rb.index_token)
            # 来源登记序号递增，说明是新的登记事件
            rows = L.daily_sources(business_date=day(0))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["index_token"], rb.index_token)

    # ==================================================================
    # 7. 规则 / 表声明不一致即拒绝
    # ==================================================================

    def test_rejects_inconsistent_ast_or_schema(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            idx = self.build_index({"A": "true"})
            # 不同 AST（阈值改了）
            bad_ast = json.loads(json.dumps(AST))
            bad_ast["args"][1]["arg"]["args"][0]["value"] = 30
            with self.assertRaises(LedgerError):
                L._check_config(bad_ast, PARENT_SCHEMA, CHILD_SCHEMA)
            # 不同表声明
            with self.assertRaises(LedgerError):
                L._check_config(AST, {"zone": "text"}, CHILD_SCHEMA)
            # 与索引 schema 不符的台账：索引声明不同属性
            other_idx = os.path.join(self.tmp, "other")
            os.makedirs(other_idx)
            pp = os.path.join(other_idx, "p.jsonl")
            write_jsonl(pp, [{"entity_id": "A", "region": "N"}])
            idx2 = os.path.join(other_idx, "index")
            Importer(idx2, {"region": "text"}, {}).run(pp, None)
            with self.assertRaises(LedgerError):
                L.register(day(0), idx2)
            # 一次失败的登记不改变台账
            self.assertEqual(L.stats()["days_registered"], 0)

    # ==================================================================
    # 8. 移开已用索引后再更正（只用 SQLite 持久状态）
    # ==================================================================

    def test_correction_after_moving_old_index_away(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            idx1 = self.build_index({"A": "true", "B": "true"})
            L.register(day(0), idx1)
            idx2 = self.build_index({"A": "true", "B": "true"})
            L.register(day(1), idx2)
            # 把两天的旧索引整个移出工作区
            moved = os.path.join(self.tmp, "moved")
            os.makedirs(moved)
            shutil.move(idx1, os.path.join(moved, "i1"))
            shutil.move(idx2, os.path.join(moved, "i2"))
            # 更正 day(0)：移除 A。只需当天新索引，旧状态全在 SQLite。
            idx3 = self.build_index({"B": "true"})
            L.register(day(0), idx3)
            eps = {(e["entity_id"], e["first_day"], e["last_day"], e["state"])
                   for e in L.episodes()}
            self.assertEqual(eps, {
                ("A", day(1), day(1), "true"),
                ("B", day(0), day(1), "true"),
            })
            self.assert_table_matches(L, [
                (day(0), [("B", "true")]),
                (day(1), [("A", "true"), ("B", "true")]),
            ])

    # ==================================================================
    # 9. 提交前中断：重开核对旧登记与旧区间，再验证重送成功
    # ==================================================================

    def test_abort_before_commit_rolls_back(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            L.register(day(0), self.build_index({"A": "true", "B": "true"}))
            old_eps = self.ledger_episodes_raw(L)
            old_src = self.ledger_source_map(L)
            old_days = L.object_days("A")
            idx_bad = self.build_index({"A": "false", "C": "true"})

            class Boom(RuntimeError):
                pass

            L.before_commit_hook = lambda: (_ for _ in ()).throw(Boom())
            with self.assertRaises(Boom):
                L.register(day(1), idx_bad)
            L.before_commit_hook = None
            # 同一连接内旧版完整
            self.assertEqual(self.ledger_episodes_raw(L), old_eps)
            self.assertEqual(self.ledger_source_map(L), old_src)

        # 重开（新进程等价）核对旧登记与旧区间
        with Ledger(self.db) as L2:
            self.assertEqual(self.ledger_episodes_raw(L2), old_eps)
            self.assertEqual(self.ledger_source_map(L2), old_src)
            self.assertEqual(L2.object_days("A"), old_days)
            # 没有半截 day_state 残留
            self.assertEqual(L2.object_days("C"), [])
            self.assertEqual(
                [r["business_date"] for r in L2.daily_sources()], [day(0)])
            # 重送成功
            result = L2.register(day(1), idx_bad)
            self.assertFalse(result.replaced)
            self.assertEqual(
                {(e["entity_id"], e["first_day"], e["last_day"], e["state"])
                 for e in L2.episodes()},
                {
                    ("A", day(0), day(0), "true"),
                    ("A", day(1), day(1), "false"),
                    ("B", day(0), day(0), "true"),
                    ("C", day(1), day(1), "true"),
                },
            )
            self.assertIn(day(1), self.ledger_source_map(L2))

    # ==================================================================
    # 10. 并发读快照：一个读事务中只看到完整旧版或完整新版
    # ==================================================================

    def test_concurrent_reader_snapshot_never_mixed(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            L.register(day(0), self.build_index(
                {"A": "true", "B": "true", "C": "true"}))
            idx_new = self.build_index({"A": "false", "B": "false"})

            # 独立的只读连接，显式开读事务，在写事务前后各读一次快照。
            ro = sqlite3.connect(self.db, isolation_level=None)
            try:
                ro.execute("BEGIN")
                before = ro.execute(
                    "SELECT entity_id, first_day, last_day, state "
                    "FROM episode ORDER BY entity_id").fetchall()
                ro.execute("COMMIT")
            finally:
                ro.close()

            L.register(day(0), idx_new)

            ro = sqlite3.connect(self.db, isolation_level=None)
            try:
                ro.execute("BEGIN")
                after = ro.execute(
                    "SELECT entity_id, first_day, last_day, state "
                    "FROM episode ORDER BY entity_id").fetchall()
                ro.execute("COMMIT")
            finally:
                ro.close()
            self.assertEqual(before, [
                ("A", day(0), day(0), "true"),
                ("B", day(0), day(0), "true"),
                ("C", day(0), day(0), "true"),
            ])
            self.assertEqual(after, [
                ("A", day(0), day(0), "false"),
                ("B", day(0), day(0), "false"),
            ])

    def test_mid_write_reader_sees_old_version(self):
        # 在写事务进行中（未提交）从另一连接读取，只能看到旧版。
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            L.register(day(0), self.build_index({"A": "true"}))
            seen = {}

            def hook():
                ro = sqlite3.connect(self.db, isolation_level=None)
                try:
                    seen["episode"] = ro.execute(
                        "SELECT entity_id, state FROM episode").fetchall()
                    seen["source"] = ro.execute(
                        "SELECT index_token FROM daily_source").fetchall()
                finally:
                    ro.close()

            L.before_commit_hook = hook
            L.register(day(0), self.build_index({"A": "false"}))
            L.before_commit_hook = None
            # 提交前的另一连接只看到旧版 true
            self.assertEqual(seen["episode"], [("A", "true")])
            # 提交后才是新版 false
            self.assertEqual(
                [(e["entity_id"], e["state"]) for e in L.episodes()],
                [("A", "false")])

    # ==================================================================
    # 11. 未扫描旧历史：两份可生成历史对照
    # ==================================================================

    def _history_work(self, n_unrelated: int, correction_day: int,
                      n_days: int):
        """生成一段历史，在第 correction_day 天做一次同日更正，返回读数。

        n_unrelated 为无关历史长度（只含其它对象，且都远离被更正对象）。
        """
        db = os.path.join(self.tmp, f"h_{n_unrelated}_{correction_day}.db")
        with Ledger(db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            # 长历史：对象 "TARGET" 每天 true（含更正日）
            # 另有 n_unrelated 个无关对象，每天状态与 TARGET 无关。
            for i in range(n_days):
                present = {"TARGET": "true"}
                for k in range(n_unrelated):
                    present[f"U{k}"] = "true" if (k + i) % 2 else "false"
                L.register(day(i), self.build_index(present))
            # 对 correction_day 做同日更正：TARGET true -> false，
            # 无关对象维持原判定（不引起它们的区间变化）。
            present = {"TARGET": "false"}
            for k in range(n_unrelated):
                present[f"U{k}"] = "true" if (k + correction_day) % 2 else "false"
            idx = self.build_index(present)
            r = L.register(day(correction_day), idx)
            eps = self.ledger_episodes_raw(L)
            return r, eps, L.stats()

    def test_update_does_not_scan_old_history(self):
        # 对照一：同样的“一天更正”，不同长度的无关历史。
        r_short, eps_short, _ = self._history_work(
            n_unrelated=5, correction_day=20, n_days=41)
        r_long, eps_long, _ = self._history_work(
            n_unrelated=400, correction_day=20, n_days=41)
        # 被更正对象 TARGET 的受影响区间段调整次数相同
        self.assertEqual(r_short.episode_adjustments,
                         r_long.episode_adjustments)
        # 区间查询步数不随无关历史规模增长（同一对象、同一日期、同样更正）
        self.assertEqual(r_short.metrics.episode_vm_steps,
                         r_long.metrics.episode_vm_steps)
        # 无关对象更多 → 日状态更多，但区间维护只碰 TARGET 在 d 处的段
        self.assertEqual(r_short.metrics.episode_rows_read,
                         r_long.metrics.episode_rows_read)

        # 对照二：同一个更正日，TARGET 的历史段长度不同。
        # 用完全不同长度的 TARGET 历史（更正点距两端远端长度不同），
        # 区间维护步数不随段长度增长。
        short_db = os.path.join(self.tmp, "target_short.db")
        long_db = os.path.join(self.tmp, "target_long.db")
        with Ledger(short_db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            L.register(day(0), self.build_index({"TARGET": "true"}))
            L.register(day(1), self.build_index({"TARGET": "true"}))
            r_s = L.register(day(1), self.build_index({"TARGET": "false"}))
        with Ledger(long_db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            for i in range(60):  # 60 天同态长段
                L.register(day(i), self.build_index({"TARGET": "true"}))
            r_l = L.register(day(59), self.build_index({"TARGET": "false"}))
        self.assertEqual(r_s.metrics.episode_vm_steps,
                         r_l.metrics.episode_vm_steps)
        self.assertEqual(r_s.metrics.episode_lookup_statements,
                         r_l.metrics.episode_lookup_statements)
        # 长历史更正后：前面 59 天仍是一段，末段独立
        with Ledger(long_db) as L:
            segs = sorted(
                (e["first_day"], e["last_day"], e["state"])
                for e in L.episodes() if e["entity_id"] == "TARGET")
        self.assertEqual(segs, [(day(0), day(58), "true"),
                                (day(59), day(59), "false")])


    # ==================================================================
    # 12. 日期只取参数：非法格式一律拒绝
    # ==================================================================

    def test_invalid_business_date_rejected(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            idx = self.build_index({"A": "true"})
            for bad in ("2026-3-1", "2026/03/01", "20260301",
                        "2026-02-30", "", "2026-13-01", "2026-03-01T00:00"):
                with self.assertRaises(LedgerError, msg=bad):
                    L.register(bad, idx)
            self.assertEqual(L.stats()["days_registered"], 0)

    def test_register_ast_argument_must_match(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            idx = self.build_index({"A": "true"})
            bad = json.loads(json.dumps(AST))
            bad["args"][1]["arg"]["args"][0]["value"] = 99
            with self.assertRaises(LedgerError):
                L.register(day(0), idx, ast=bad)
            # 正确的规则可以通过
            L.register(day(0), idx, ast=AST)
            self.assertEqual(L.stats()["days_registered"], 1)

    # ==================================================================
    # 13. 延迟补录很多天后，单日更正仍只碰受影响段
    # ==================================================================

    def test_deep_history_single_day_correction_is_local(self):
        with Ledger(self.db) as L:
            L.create(AST, PARENT_SCHEMA, CHILD_SCHEMA)
            # 30 天 TARGET=true，形成一段 30 天的长段
            for i in range(30):
                L.register(day(i), self.build_index(
                    {"TARGET": "true", "OTHER": "true"}))
            # 更正第 15 天：只 TARGET 变 false
            r = L.register(day(15), self.build_index(
                {"TARGET": "false", "OTHER": "true"}))
            # 仅 TARGET 一个对象受影响；查找语句固定为常数级
            self.assertEqual(r.changed_entities, 1)
            self.assertLessEqual(r.metrics.episode_lookup_statements, 4)
            segs = sorted(
                (e["first_day"], e["last_day"], e["state"])
                for e in L.episodes() if e["entity_id"] == "TARGET")
            self.assertEqual(segs, [
                (day(0), day(14), "true"),
                (day(15), day(15), "false"),
                (day(16), day(29), "true"),
            ])
            # OTHER 完全不受影响，仍是一段
            self.assertEqual(
                [(e["first_day"], e["last_day"])
                 for e in L.episodes() if e["entity_id"] == "OTHER"],
                [(day(0), day(29))])


if __name__ == "__main__":
    unittest.main()
