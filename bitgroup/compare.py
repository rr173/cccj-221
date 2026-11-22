"""新旧规则历史对照：两份台账的只读九格对照，输出按日期压缩的变更带。

解决的问题
==========

试跑期间，同一批日报索引被两套规则各自记进一份台账（左=老规则，右=新
规则）。上线前要知道：**同一批对象在两套规则下分别落入哪一格，这些格子
的成员是否稳定**。本模块把两份现成的 SQLite 台账与一个闭合自然日窗口
作为输入，产出一份 JSONL 变更带：窗口内每个可比日的 ``(左状态, 右状态)``
九格人数，以及每格“前一天不在同一格”的进入人数。

语义要点
========

* **可比性只看当日来源代号**：两边 ``daily_source`` 都登记了这一天且
  ``index_token`` 完全相等 → ``comparable``；都有登记但代号不同 →
  ``different_source``；只有一边登记 → ``left_missing`` / ``right_missing``
  （指缺登记的一边）；都没有 → ``both_missing``。只认**当天**的代号，
  同一代号出现在别的日期不算；不看人数、不看规则指纹。
* **九格归格**：每个可比日，按原始 ``entity_id`` 对齐两边覆盖该日的
  ``episode``，同一对象落入 ``(左状态, 右状态)`` 一格。状态取
  ``true`` / ``false`` / ``unknown``，固定九格。只计入**两侧当日都有
  区间**的对象；缺席不算 ``unknown``。同一对象当天两边同时变状态，直接
  按新状态对归格，没有中间格。
* **count / entered**：``count`` 是当天在该格的人数；``entered`` 是其中
  **前一自然日未在同一格**的人数（包括前一天不在对照对象内的人）。若前
  一日不在窗口内或不可比，则当天九格 ``entered`` 全为 ``null``（不是零，
  也不回溯到较早的可比日）。
* **变更带压缩**：每行给出 ``first_day`` / ``last_day`` / ``status`` /
  ``cells``，日期 ``YYYY-MM-DD`` 两端都含。不可比段 ``cells`` 为
  ``null``；可比段给出完整九格（含零人数格）。整段窗口无重无漏、按时间
  递增；只有 ``status`` 与整组 ``cells`` 都相同的相邻自然日才合并，且
  必须合并。

只读与快照
==========

一次 ``compare_ledgers`` 对每份台账各开一个 **只读连接**（``mode=ro`` +
``PRAGMA query_only``），并在构造时 ``BEGIN`` 一个读事务：来源登记与状态
区间永远来自**同一份提交**（WAL 读快照），台账可以边用边继续登记。命令
不修改台账内容，也不需要打开任何日报索引或原始文件；若只读连接为 WAL
恢复创建了空的 ``-shm``/``-wal`` 边车文件，结束时负责清理（只清理自己
创建、且 wal 仍为空的那一份）。

实现与规模
==========

主要工作量由**窗口相关的状态区间记录**与**日来源记录**决定：

* 区间读取是对 ``episode_segment`` 的一次顺序筛选（沿主键序扫描，窗口外
  记录直接跳过），不把台账还原成逐对象逐日的记录；
* 每个对象的左/右区间流式归并成若干“状态对游程”（pair-run），只向磁盘
  暂存表写游程端点事件（开始日 +1、结束次日 -1、开始日记一次 entered），
  由 SQLite 磁盘排序后按日扫描聚合——长区间**不会**被复制成与每段可比日
  分别相交的中间记录；零散的未对齐日期只影响来源状态流，不会放大稳定
  对象的工作量；
* 来源登记按日主键序双路归并，直接产出压缩后的状态游程；最终输出按
  （状态游程 × 事件日）归并，全程不常驻全部对象、区间或整条日期轴。

关键恒等式：在可比日 ``d``（``d-1`` 也在窗口内且可比），格 ``c`` 的
``entered`` 恰好等于**起始日为 d 的状态对游程数**——对象当天在该格且
前一天不在同一格，当且仅当它的状态对游程从 d 开始。
"""

from __future__ import annotations

import datetime
import os
import pathlib
import shutil
import sqlite3
import tempfile
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .errors import LedgerError
from .ledger import parse_business_date

# 三态与九格：格子顺序固定（左外层、右内层），左右坐标在输出中显式可辨。
CELL_STATES = ("true", "false", "unknown")
CELLS: Tuple[Tuple[str, str], ...] = tuple(
    (ls, rs) for ls in CELL_STATES for rs in CELL_STATES
)
_CELL_INDEX = {c: i for i, c in enumerate(CELLS)}
ZERO9 = (0, 0, 0, 0, 0, 0, 0, 0, 0)

STATUS_COMPARABLE = "comparable"
STATUS_DIFFERENT_SOURCE = "different_source"
STATUS_LEFT_MISSING = "left_missing"
STATUS_RIGHT_MISSING = "right_missing"
STATUS_BOTH_MISSING = "both_missing"
STATUSES = (
    STATUS_COMPARABLE,
    STATUS_DIFFERENT_SOURCE,
    STATUS_LEFT_MISSING,
    STATUS_RIGHT_MISSING,
    STATUS_BOTH_MISSING,
)

_EVENT_CHUNK = 8192
_VM_GRANULARITY = 100


def _ord_to_day(ordinal: int) -> str:
    return datetime.date.fromordinal(ordinal).isoformat()


class _ReadView:
    """一份台账的只读快照连接：mode=ro + query_only + 显式读事务。

    WAL 下读事务固定快照：来源登记与区间读永远来自同一份提交，台账可以
    边用边继续登记。若 SQLite 为 WAL 恢复自行创建了 ``-shm``/``-wal``
    边车（空 wal），close 时只清理自己创建的那一份。
    """

    def __init__(self, path: str, metrics: Dict[str, Any], side: str):
        if not os.path.exists(path):
            raise LedgerError(f"台账不存在: {path}")
        self.path = path
        self._wal = path + "-wal"
        self._shm = path + "-shm"
        self._sidecars_preexisting = {
            p for p in (self._wal, self._shm) if os.path.exists(p)
        }
        uri = pathlib.Path(path).absolute().as_uri() + "?mode=ro"
        self.conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        try:
            self.conn.row_factory = sqlite3.Row
            self.conn.execute("PRAGMA busy_timeout=60000")
            self.conn.execute("PRAGMA query_only=ON")
            self._metrics = metrics
            self._side = side
            self._vm_steps = 0
            self.conn.set_progress_handler(self._on_vm_step, _VM_GRANULARITY)
            # 固定读视图：此后本连接所有读都来自同一份提交。
            self.conn.execute("BEGIN")
        except BaseException:
            self.conn.close()
            raise

    def _on_vm_step(self) -> int:
        self._vm_steps += 1
        return 0

    def close(self) -> None:
        try:
            self.conn.set_progress_handler(None, 0)
            self.conn.execute("ROLLBACK")  # 只读事务，回滚即释放快照
            self.conn.close()
        finally:
            self._metrics[self._side]["vm_steps"] = self._vm_steps
            self._metrics[self._side]["vm_granularity"] = _VM_GRANULARITY
            self._cleanup_sidecars()

    def _cleanup_sidecars(self) -> None:
        # 只清理本次打开前不存在、且 wal 仍为空的边车；正在被别人使用的
        # （打开时已存在，或 wal 已有内容）一律保留。
        if self._wal in self._sidecars_preexisting:
            return
        try:
            if os.path.exists(self._wal) and os.path.getsize(self._wal) == 0:
                os.remove(self._wal)
                if self._shm not in self._sidecars_preexisting and \
                        os.path.exists(self._shm):
                    os.remove(self._shm)
        except OSError:
            pass  # 清理失败不影响结果；边车由后续写者正常回收


def _iter_intervals(
    conn: sqlite3.Connection,
    s: int,
    e: int,
    side_metrics: Dict[str, Any],
) -> Iterator[Tuple[str, int, int, str]]:
    """沿 (entity_id, first_ord) 主键序顺序扫描区间表，窗口外记录跳过。

    这是一次顺序筛选：不逐对象展开成逐日记录，也不为每个可比日段复制
    区间。产出 (entity_id, first_ord, last_ord, state)。
    """
    cur = conn.execute(
        "SELECT entity_id, first_ord, last_ord, state FROM episode_segment "
        "WHERE last_ord >= ? AND first_ord <= ? "
        "ORDER BY entity_id, first_ord",
        (s, e),
    )
    last_eid: Optional[str] = None
    for row in cur:
        side_metrics["episode_rows_in_window"] += 1
        eid = row["entity_id"]
        if eid != last_eid:
            side_metrics["entities_in_window"] += 1
            last_eid = eid
        yield (eid, int(row["first_ord"]), int(row["last_ord"]), row["state"])


def _entity_groups(
    rows: Iterator[Tuple[str, int, int, str]]
) -> Iterator[Tuple[str, List[Tuple[int, int, str]]]]:
    """把按 entity_id 排序的区间行归组为 (entity_id, [区间...])，流式。"""
    cur_eid: Optional[str] = None
    buf: List[Tuple[int, int, str]] = []
    for eid, f, l, st in rows:
        if eid != cur_eid:
            if cur_eid is not None:
                yield cur_eid, buf
            cur_eid = eid
            buf = []
        buf.append((f, l, st))
    if cur_eid is not None:
        yield cur_eid, buf


def _pair_runs(
    left: List[Tuple[int, int, str]],
    right: List[Tuple[int, int, str]],
    s: int,
    e: int,
) -> List[Tuple[int, int, int]]:
    """同一对象的左/右区间（各自有序、互不相交）归并为状态对游程。

    只保留两侧都有区间的日子；游程是 (左状态, 右状态) 保持不变的最大
    自然日段，裁剪到 [s, e]。返回 [(first_ord, last_ord, cell_index)]。
    复杂度 O(左段数 + 右段数)，与该对象覆盖的天数无关。
    """
    lc = [(max(f, s), min(l, e), st) for f, l, st in left]
    rc = [(max(f, s), min(l, e), st) for f, l, st in right]
    runs: List[List[Any]] = []
    i = j = 0
    while i < len(lc) and j < len(rc):
        la, lb, ls = lc[i]
        ra, rb, rs = rc[j]
        a = la if la > ra else ra
        b = lb if lb < rb else rb
        if a <= b:
            ci = _CELL_INDEX[(ls, rs)]
            if runs and runs[-1][2] == ci and runs[-1][1] == a - 1:
                runs[-1][1] = b
            else:
                runs.append([a, b, ci])
        if lb < rb:
            i += 1
        elif rb < lb:
            j += 1
        else:
            i += 1
            j += 1
    return [(a, b, ci) for a, b, ci in runs]


class CompareRun:
    """一次对照的运行时：持有两份台账的只读快照与磁盘暂存。

    构造时即固定两份台账各自的读视图，并完成“区间 → 状态对游程 → 事件”
    的第一遍（流式，事件落磁盘暂存）；:meth:`rows` 做第二遍归并并流式
    产出变更带行（只能消费一次）。``metrics`` 在迭代结束后完整。
    """

    def __init__(
        self,
        left_path: str,
        right_path: str,
        first_day: str,
        last_day: str,
        *,
        scratch_dir: Optional[str] = None,
    ):
        s_text, s = parse_business_date(first_day)
        e_text, e = parse_business_date(last_day)
        if e < s:
            raise LedgerError(f"窗口端点颠倒: {first_day!r} 晚于 {last_day!r}")
        self._s, self._e = s, e
        self._s_text, self._e_text = s_text, e_text
        self.metrics: Dict[str, Any] = {
            "window": {"first_day": s_text, "last_day": e_text,
                       "days": e - s + 1},
            "left": {"daily_source_rows": 0, "episode_rows_in_window": 0,
                     "entities_in_window": 0},
            "right": {"daily_source_rows": 0, "episode_rows_in_window": 0,
                      "entities_in_window": 0},
            "entities_compared": 0,
            "pair_runs": 0,
            "events_written": 0,
            "event_days": 0,
            "status_runs": 0,
            "status_day_counts": {st: 0 for st in STATUSES},
            "status_run_counts": {st: 0 for st in STATUSES},
            "output_rows": 0,
            "scratch": {},
        }
        self._scratch_vm_steps = 0
        self._rows_started = False
        self._tmpdir = tempfile.mkdtemp(prefix="bitgroup_compare_",
                                        dir=scratch_dir)
        self._closed = False
        try:
            self._left = _ReadView(left_path, self.metrics, "left")
            self._right = _ReadView(right_path, self.metrics, "right")
            self._scratch = sqlite3.connect(
                os.path.join(self._tmpdir, "scratch.db"), isolation_level=None)
            self._scratch.execute("PRAGMA temp_store=FILE")
            self._scratch.execute("PRAGMA journal_mode=OFF")
            self._scratch.execute("PRAGMA synchronous=OFF")
            self._scratch.set_progress_handler(self._on_scratch_step,
                                               _VM_GRANULARITY)
            self._scratch.execute(
                "CREATE TABLE ev (day_ord INTEGER NOT NULL, "
                "cell INTEGER NOT NULL, cd INTEGER NOT NULL, "
                "sd INTEGER NOT NULL)"
            )
            self._emit_events()
        except BaseException:
            self.close()
            raise

    # ---- 生命周期 ------------------------------------------------------

    def __enter__(self) -> "CompareRun":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for view in (getattr(self, "_left", None),
                     getattr(self, "_right", None)):
            if view is not None:
                view.close()
        scratch = getattr(self, "_scratch", None)
        if scratch is not None:
            scratch.set_progress_handler(None, 0)
            scratch.close()
            self.metrics["scratch"]["vm_steps"] = self._scratch_vm_steps
            self.metrics["scratch"]["vm_granularity"] = _VM_GRANULARITY
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _on_scratch_step(self) -> int:
        self._scratch_vm_steps += 1
        return 0

    # ---- 第一遍：区间 → 状态对游程 → 端点事件（磁盘暂存） ---------------

    def _emit_events(self) -> None:
        s, e = self._s, self._e
        left_groups = _entity_groups(_iter_intervals(
            self._left.conn, s, e, self.metrics["left"]))
        right_groups = _entity_groups(_iter_intervals(
            self._right.conn, s, e, self.metrics["right"]))
        lg = next(left_groups, None)
        rg = next(right_groups, None)
        chunk: List[Tuple[int, int, int, int]] = []

        def flush() -> None:
            if chunk:
                self._scratch.executemany(
                    "INSERT INTO ev (day_ord, cell, cd, sd) "
                    "VALUES (?, ?, ?, ?)", chunk)
                self.metrics["events_written"] += len(chunk)
                chunk.clear()

        # 按 entity_id 双路归并；只在一边出现的对象永不进入对照集。
        while lg is not None and rg is not None:
            if lg[0] < rg[0]:
                lg = next(left_groups, None)
                continue
            if rg[0] < lg[0]:
                rg = next(right_groups, None)
                continue
            runs = _pair_runs(lg[1], rg[1], s, e)
            self.metrics["entities_compared"] += 1
            self.metrics["pair_runs"] += len(runs)
            for a, b, ci in runs:
                chunk.append((a, ci, 1, 1))          # 游程开始：入格 + 进入
                if b < e:
                    chunk.append((b + 1, ci, -1, 0))  # 游程结束次日：出格
                if len(chunk) >= _EVENT_CHUNK:
                    flush()
            lg = next(left_groups, None)
            rg = next(right_groups, None)
        flush()

    # ---- 第二遍：日来源状态游程 -----------------------------------------

    def _source_days(
        self, view: _ReadView, side: str
    ) -> Iterator[Tuple[int, str]]:
        cur = view.conn.execute(
            "SELECT business_date, index_token FROM daily_source "
            "WHERE business_date >= ? AND business_date <= ? "
            "ORDER BY business_date",
            (self._s_text, self._e_text),
        )
        for row in cur:
            self.metrics[side]["daily_source_rows"] += 1
            _t, o = parse_business_date(row["business_date"])
            yield o, row["index_token"]

    def _merged_source_days(
        self,
    ) -> Iterator[Tuple[int, Optional[str], Optional[str]]]:
        """双路归并两边 daily_source，产出 (ord, 左代号|None, 右代号|None)。

        只含至少一边有登记的日期，按日升序，流式不物化整条日期轴。
        """
        lit = iter(self._source_days(self._left, "left"))
        rit = iter(self._source_days(self._right, "right"))
        l = next(lit, None)
        r = next(rit, None)
        while l is not None or r is not None:
            if r is None or (l is not None and l[0] < r[0]):
                yield l[0], l[1], None
                l = next(lit, None)
            elif l is None or r[0] < l[0]:
                yield r[0], None, r[1]
                r = next(rit, None)
            else:
                yield l[0], l[1], r[1]
                l = next(lit, None)
                r = next(rit, None)

    def _status_runs(self) -> Iterator[Tuple[int, int, str]]:
        """把归并后的登记日压缩成覆盖整个窗口的状态游程。

        登记日之间的空档直接以 both_missing 游程填平，不逐日迭代。
        """
        run_start: Optional[int] = None
        run_status: Optional[str] = None
        covered = self._s - 1  # 已覆盖到的最后一天（含）

        def push(a: int, st: str):
            # a 必须 == covered+1；同态则扩展当前游程，否则关闭并新开。
            nonlocal run_start, run_status
            if run_status is not None and run_status == st:
                return None
            out = (run_start, a - 1, run_status) \
                if run_status is not None else None
            run_start, run_status = a, st
            return out

        for o, lt, rt in self._merged_source_days():
            if o > covered + 1:
                flushed = push(covered + 1, STATUS_BOTH_MISSING)
                if flushed is not None:
                    yield flushed
                covered = o - 1
            if lt is not None and rt is not None:
                st = (STATUS_COMPARABLE if lt == rt
                      else STATUS_DIFFERENT_SOURCE)
            elif lt is None:
                st = STATUS_LEFT_MISSING
            else:
                st = STATUS_RIGHT_MISSING
            flushed = push(o, st)
            if flushed is not None:
                yield flushed
            covered = o
        if covered < self._e:
            flushed = push(covered + 1, STATUS_BOTH_MISSING)
            if flushed is not None:
                yield flushed
            covered = self._e
        if run_status is not None:
            yield (run_start, covered, run_status)

    # ---- 事件聚合（磁盘排序后按日扫描） ----------------------------------

    def _event_days(self) -> Iterator[Tuple[int, List[int], List[int]]]:
        """产出 (day_ord, count_delta[9], start_delta[9])，按日升序。"""
        cur = self._scratch.execute(
            "SELECT day_ord, cell, SUM(cd), SUM(sd) FROM ev "
            "GROUP BY day_ord, cell ORDER BY day_ord, cell")
        day: Optional[int] = None
        cd = [0] * 9
        sd = [0] * 9
        for d, c, dv, sv in cur:
            if day is None:
                day = int(d)
            if int(d) != day:
                self.metrics["event_days"] += 1
                yield day, cd, sd
                cd = [0] * 9
                sd = [0] * 9
                day = int(d)
            cd[int(c)] += int(dv)
            sd[int(c)] += int(sv)
        if day is not None:
            self.metrics["event_days"] += 1
            yield day, cd, sd

    # ---- 最终归并：状态游程 × 事件日 → 压缩变更带 -------------------------

    @staticmethod
    def _row_from(span) -> Dict[str, Any]:
        a, b, status, key = span
        row: Dict[str, Any] = {
            "first_day": _ord_to_day(a),
            "last_day": _ord_to_day(b),
            "status": status,
        }
        if key is None:
            row["cells"] = None
        else:
            counts, entered = key
            row["cells"] = [
                {
                    "left": ls,
                    "right": rs,
                    "count": counts[i],
                    "entered": None if entered is None else entered[i],
                }
                for i, (ls, rs) in enumerate(CELLS)
            ]
        return row

    def rows(self) -> Iterator[Dict[str, Any]]:
        """流式产出变更带行（每行一个 dict）。只能消费一次。"""
        if self._rows_started:
            raise LedgerError("变更带只能顺序消费一次")
        self._rows_started = True
        s, e = self._s, self._e
        counts = [0] * 9
        starts: List[int] = list(ZERO9)
        ev_iter = self._event_days()
        ev = next(ev_iter, None)

        def advance_to(day: int) -> None:
            # 应用 day（含）之前的所有事件；starts 记录 day 当天的进入数。
            nonlocal ev, starts
            starts = list(ZERO9)
            while ev is not None and ev[0] <= day:
                d, cd, sd = ev
                for i in range(9):
                    counts[i] += cd[i]
                if d == day:
                    starts = sd
                ev = next(ev_iter, None)

        cur: Optional[list] = None  # [start, end, status, key]

        def emit(a: int, b: int, status: str, key):
            # 相邻且 (status, cells) 全同则合并（必须合并），否则冲洗上一段。
            nonlocal cur
            if (cur is not None and cur[2] == status and cur[3] == key
                    and cur[1] + 1 == a):
                cur[1] = b
                return None
            out = cur
            cur = [a, b, status, key]
            return out

        for a, b, st in self._status_runs():
            self.metrics["status_runs"] += 1
            self.metrics["status_day_counts"][st] += b - a + 1
            self.metrics["status_run_counts"][st] += 1
            if st != STATUS_COMPARABLE:
                advance_to(b)
                flushed = emit(a, b, st, None)
                if flushed is not None:
                    self.metrics["output_rows"] += 1
                    yield self._row_from(flushed)
                continue
            # 可比游程：首日 entered 必为 null（前一日在窗口外或不可比）。
            advance_to(a)
            flushed = emit(a, a, st, (tuple(counts), None))
            if flushed is not None:
                self.metrics["output_rows"] += 1
                yield self._row_from(flushed)
            d = a + 1
            while d <= b:
                if ev is not None and ev[0] <= b:
                    evd = ev[0]
                    if d <= evd - 1:
                        flushed = emit(d, evd - 1, st, (tuple(counts), ZERO9))
                        if flushed is not None:
                            self.metrics["output_rows"] += 1
                            yield self._row_from(flushed)
                    advance_to(evd)
                    flushed = emit(evd, evd, st,
                                   (tuple(counts), tuple(starts)))
                    if flushed is not None:
                        self.metrics["output_rows"] += 1
                        yield self._row_from(flushed)
                    d = evd + 1
                else:
                    flushed = emit(d, b, st, (tuple(counts), ZERO9))
                    if flushed is not None:
                        self.metrics["output_rows"] += 1
                        yield self._row_from(flushed)
                    d = b + 1
        if cur is not None:
            self.metrics["output_rows"] += 1
            yield self._row_from(cur)


def compare_ledgers(
    left_path: str,
    right_path: str,
    first_day: str,
    last_day: str,
    *,
    scratch_dir: Optional[str] = None,
) -> CompareRun:
    """打开两份台账的只读对照。窗口两端日期都包含。"""
    return CompareRun(left_path, right_path, first_day, last_day,
                      scratch_dir=scratch_dir)
