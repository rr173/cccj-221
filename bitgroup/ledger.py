"""历史状态台账：按自然日登记三态判定，维护可直读的状态区间。

解决的问题
==========

质检要问“某对象从哪天起连续处于待核状态？中间有没有断档？”。原取群
命令只回答“当前某个索引里的判定”，不保留跨时间的历史，也无法回答跨日
连续段。台账把**日结**（某业务日期 + 当天只读索引 + 固定查询 AST）登记
进 SQLite，并维护每个对象在自然日轴上**连续且同态**的区间：日结可乱序
补录、可整体替换（更正版），已登记的日结还能用更正版重送。

设计要点
========

* **一份台账固定一个 AST 和一套主/子表声明**：首次建台账时把规范化 AST
  及其指纹、主/子 schema 写入 ``ledger_meta``；之后的命令若 AST 或 schema
  与台账不一致直接拒绝。AST 本身也原样保存，补录时不必再传规则文件。
* **日期只来自命令参数**：``business_date`` 必须显式给出 ``YYYY-MM-DD``，
  绝不从导入顺序、目录名或文件时间推断。
* **跨日以原始 entity_id 相认**：每个索引各自有局部编号（pid），台账只
  用索引身份层把 pid 翻译回原始字符串，绝不跨索引沿用局部编号。
* **状态守恒**：某日某对象只有登记为 ``true`` / ``false`` / ``unknown``
  才进入日状态；**没有登记该日** 或 **当日已登记但没有该对象** 都会切断
  连续性，两者都不产出状态行，绝不当作 false/unknown。
* **每行覆盖连续同态的一段自然日，两端闭合**：相邻且同态的段立即合并。
* **原子与并发**：每次更新是一个 ``BEGIN IMMEDIATE`` 写事务；失败则整
  体回滚，WAL 下读者要么看到完整旧版、要么看到完整新版，不会出现
  “新登记配旧区间”。增量在写事务内基于**已提交**的旧状态计算，因此并发
  写串行叠加，不会用陈旧计划覆盖他人结果。
* **增量维护**：只打开本次送来的索引；对照当天旧/新结果找出新增、消失
  或变态的对象，只对这些对象在该日期处受影响的段做查找与调整，不扫描
  全部历史日期，也不重建整张表。当日提取与比对按对象流式进行，内存随
  当天变化量而非全库区间增长。原始日报索引移走后，后续更新只用 SQLite
  中已持久化的状态和边界，无需旧索引。

对外直读接口（SQLite）
======================

``episode(entity_id, first_day, last_day, state)``
    视图；两端日期都包含，按对象、起始日排序。每行是一段连续同态自然日。

``daily_source(business_date, index_token, ast_fingerprint, registered_at)``
    视图；按日来源登记。下游据此区分“当天缺报”和“当天已登记但没有该
    对象”（后者在该日 ``day_state`` 中无该对象行）。
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import astutil
from .errors import LedgerError

_SCHEMA_VERSION = 1
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_STATES = ("true", "false", "unknown")

_SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS ledger_meta (
        k TEXT PRIMARY KEY,
        v TEXT NOT NULL
    )""",
    # 每个 (业务日期, 对象) 的三态判定。date_ord 为自然日纪元序数，
    # 便于按日期直接定位区间边界，避免遍历其它日期。
    """CREATE TABLE IF NOT EXISTS day_state (
        business_date TEXT NOT NULL,
        date_ord      INTEGER NOT NULL,
        entity_id     TEXT NOT NULL,
        state         TEXT NOT NULL CHECK (state IN ('true','false','unknown')),
        PRIMARY KEY (business_date, entity_id)
    ) WITHOUT ROWID""",
    "CREATE INDEX IF NOT EXISTS day_state_by_date ON day_state (date_ord)",
    "CREATE INDEX IF NOT EXISTS day_state_by_entity "
    "ON day_state (entity_id, date_ord)",
    # 来源登记：每个业务日期一行，记录该日实际登记/替换的索引代号。
    """CREATE TABLE IF NOT EXISTS day_source (
        business_date   TEXT PRIMARY KEY,
        date_ord        INTEGER NOT NULL,
        index_token     TEXT NOT NULL,
        ast_fingerprint TEXT NOT NULL,
        registered_at   TEXT NOT NULL,
        update_seq      INTEGER NOT NULL
    ) WITHOUT ROWID""",
    "CREATE INDEX IF NOT EXISTS day_source_by_token "
    "ON day_source (index_token)",
    # 由 day_state 增量维护的连续同态区间（first_ord/last_ord 两端闭合）。
    """CREATE TABLE IF NOT EXISTS episode_segment (
        entity_id TEXT NOT NULL,
        first_ord INTEGER NOT NULL,
        last_ord  INTEGER NOT NULL,
        first_day TEXT NOT NULL,
        last_day  TEXT NOT NULL,
        state     TEXT NOT NULL CHECK (state IN ('true','false','unknown')),
        PRIMARY KEY (entity_id, first_ord)
    ) WITHOUT ROWID""",
    "CREATE INDEX IF NOT EXISTS episode_segment_by_end "
    "ON episode_segment (entity_id, last_ord)",
    # 下游直读视图：列名与题目要求的 episode(...) 完全一致。
    """CREATE VIEW IF NOT EXISTS episode AS
       SELECT entity_id, first_day, last_day, state FROM episode_segment""",
    # 下游直读视图：按日来源登记。
    """CREATE VIEW IF NOT EXISTS daily_source AS
       SELECT business_date, index_token, ast_fingerprint, registered_at,
              update_seq
       FROM day_source""",
]

_META_AST = "ast_json"
_META_AST_FP = "ast_fingerprint"
_META_PARENT = "parent_schema"
_META_CHILD = "child_schema"
_META_VERSION = "schema_version"

_DELTA_CHUNK = 2048
_VM_EXACT_ROWS = 64


# --------------------------------------------------------------------------
# 日期与规范化工具
# --------------------------------------------------------------------------


def parse_business_date(text: str) -> Tuple[str, int]:
    """严格解析 ``YYYY-MM-DD``，返回 (规范化文本, 纪元自然日序数)。

    只接受命令显式给出的字符串；不做时区/本地时间换算，避免从运行环境
    猜测日期。
    """
    import datetime

    if not isinstance(text, str) or not _DATE_RE.match(text):
        raise LedgerError(f"业务日期必须形如 YYYY-MM-DD，得到 {text!r}")
    try:
        d = datetime.date.fromisoformat(text)
    except ValueError as exc:
        raise LedgerError(f"业务日期非法: {text!r} ({exc})") from exc
    if d.isoformat() != text:
        raise LedgerError(f"业务日期必须零填充且合法: {text!r}")
    return text, d.toordinal()


def _ord_to_day(ordinal: int) -> str:
    import datetime

    return datetime.date.fromordinal(ordinal).isoformat()


def _iso_now() -> str:
    import datetime

    return datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


def _dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def read_index_schema(index_dir: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """从只读索引的 meta.json 读取主/子表声明（不打开完整引擎）。"""
    path = os.path.join(index_dir, "meta.json")
    if not os.path.exists(path):
        raise LedgerError(f"索引目录缺少 meta.json: {index_dir}")
    with open(path, "r", encoding="utf-8") as fh:
        meta = json.load(fh)
    return dict(meta["parent_schema"]), dict(meta["child_schema"])


def _deep_copy(obj: Any) -> Any:
    return json.loads(json.dumps(obj))


# --------------------------------------------------------------------------
# 结果对象
# --------------------------------------------------------------------------


@dataclass
class UpdateMetrics:
    """一次登记的增量维护可观察量（用于证明没有扫描旧历史）。"""

    day_rows_extracted: int = 0
    delta_rows: int = 0
    episode_lookup_statements: int = 0
    episode_rows_read: int = 0
    episode_rows_inserted: int = 0
    episode_rows_deleted: int = 0
    episode_rows_updated: int = 0
    day_state_rows_deleted: int = 0
    day_state_rows_inserted: int = 0
    transaction_commits: int = 1
    # SQLite 虚拟机在“区间增量维护”窗口内的执行步数（set_progress_handler
    # 计数）。与历史长度无关是“没有扫描旧历史”的直接证据。
    # granularity=1 表示逐指令精确计数；大于 1 表示按该粒度采样（步数为
    # 采样次数，真实指令数为 steps*granularity 的邻域）。
    episode_vm_steps: int = 0
    episode_vm_granularity: int = 1

    def as_dict(self) -> Dict[str, int]:
        return dict(self.__dict__)


@dataclass
class RegisterResult:
    business_date: str
    index_token: str
    replaced: bool
    changed_entities: int
    episode_adjustments: int
    source_updated: bool
    commit_seq: int
    metrics: UpdateMetrics
    io: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "business_date": self.business_date,
            "index_token": self.index_token,
            "replaced": self.replaced,
            "changed_entities": self.changed_entities,
            "episode_adjustments": self.episode_adjustments,
            "source_updated": self.source_updated,
            "commit_seq": self.commit_seq,
            "metrics": self.metrics.as_dict(),
            "io": self.io,
        }


# --------------------------------------------------------------------------
# 台账
# --------------------------------------------------------------------------


class Ledger:
    """一个 SQLite 台账。固定 AST + 主/子表声明，按业务日期登记日结。"""

    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        if parent and not os.path.isdir(parent):
            raise LedgerError(f"台账目录不存在: {parent}")
        # isolation_level=None：显式管理事务边界，保证一次更新一个写事务。
        self.conn = sqlite3.connect(path, isolation_level=None, timeout=60.0)
        self.conn.row_factory = sqlite3.Row
        # WAL：读事务固定快照，写事务串行；busy_timeout 让并发写等待而非失败。
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=60000")
        # 暂存表落到临时文件，不把全日状态装进 Python 内存或内存库。
        self.conn.execute("PRAGMA temp_store=FILE")
        self._meta: Optional[Dict[str, str]] = None
        self._m = UpdateMetrics()
        self._vm_steps = 0
        # 故障注入钩子：在提交前调用，供测试模拟“写入未提交即中断”。
        self.before_commit_hook: Optional[Callable[[], None]] = None

    # ---- 生命周期 ------------------------------------------------------

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @property
    def exists(self) -> bool:
        row = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ledger_meta'"
        ).fetchone()
        return row is not None

    def meta(self) -> Dict[str, str]:
        if self._meta is None:
            if not self.exists:
                raise LedgerError("台账尚未创建")
            self._meta = {
                r["k"]: r["v"]
                for r in self.conn.execute("SELECT k, v FROM ledger_meta")
            }
        return self._meta

    # ---- 创建 ----------------------------------------------------------

    def create(
        self,
        ast: Dict[str, Any],
        parent_schema: Dict[str, str],
        child_schema: Dict[str, str],
    ) -> None:
        """首次创建台账：固定规范化 AST 与主/子表声明。"""
        if self.exists:
            raise LedgerError("台账已存在，不能重复创建")
        node = astutil.validate_ast(_deep_copy(ast), parent_schema, child_schema)
        fp = astutil.ast_fingerprint(node)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for stmt in _SCHEMA_STATEMENTS:
                self.conn.execute(stmt)
            rows = [
                (_META_VERSION, str(_SCHEMA_VERSION)),
                (_META_AST, _dumps(node)),
                (_META_AST_FP, fp),
                (_META_PARENT, _dumps(dict(parent_schema))),
                (_META_CHILD, _dumps(dict(child_schema))),
            ]
            self.conn.executemany(
                "INSERT INTO ledger_meta (k, v) VALUES (?, ?)", rows
            )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        self._meta = None

    # ---- 与台账固定的配置核对 -----------------------------------------

    def _check_config(
        self,
        ast: Dict[str, Any],
        parent_schema: Dict[str, str],
        child_schema: Dict[str, str],
    ) -> Tuple[Dict[str, Any], str]:
        meta = self.meta()
        if meta.get(_META_PARENT) != _dumps(dict(parent_schema)):
            raise LedgerError("主表声明与台账不一致，已拒绝")
        if meta.get(_META_CHILD) != _dumps(dict(child_schema)):
            raise LedgerError("子表声明与台账不一致，已拒绝")
        try:
            node = astutil.validate_ast(
                _deep_copy(ast), parent_schema, child_schema
            )
        except Exception as exc:
            raise LedgerError(f"规则文件与表声明不匹配: {exc}") from exc
        fp = astutil.ast_fingerprint(node)
        if meta.get(_META_AST_FP) != fp:
            raise LedgerError("查询 AST 与台账固定规则不一致，已拒绝")
        return node, fp

    def fixed_ast(self) -> Dict[str, Any]:
        return json.loads(self.meta()[_META_AST])

    # ------------------------------------------------------------------
    # 登记一个日结（首次登记、补录、替换都走这里）
    # ------------------------------------------------------------------

    def register(
        self,
        business_date: str,
        index_dir: str,
        *,
        ast: Optional[Dict[str, Any]] = None,
        engine_factory: Optional[Callable[[str], Any]] = None,
    ) -> RegisterResult:
        """登记一个日结。

        ``ast`` 若提供则必须与台账固定规则一致（不一致直接拒绝）；台账
        始终使用自身固定的 AST 执行。日期只取 ``business_date`` 参数。
        """
        date_text, date_ord = parse_business_date(business_date)
        self.meta()  # 确保台账已创建
        if engine_factory is None:
            from .engine import Engine as engine_factory  # type: ignore
        if ast is not None:
            # 用台账固定的 schema 校验传入规则；指纹不一致即拒绝。
            meta = self.meta()
            self._check_config(
                ast,
                json.loads(meta[_META_PARENT]),
                json.loads(meta[_META_CHILD]),
            )
        self._m = UpdateMetrics()
        with engine_factory(index_dir) as eng:
            _node, fp = self._check_config(
                self.fixed_ast(), eng.parent_schema, eng.child_schema
            )
            io = self._extract_new_day(eng)
            return self._apply(date_text, date_ord, eng.index_token, fp, io)

    # ------------------------------------------------------------------
    # 第一遍（事务外）：流式提取当天结果到临时表
    # ------------------------------------------------------------------

    def _extract_new_day(self, eng) -> Dict[str, Any]:
        self.conn.execute("DROP TABLE IF EXISTS temp.new_day")
        self.conn.execute(
            "CREATE TEMP TABLE new_day ("
            "entity_id TEXT PRIMARY KEY, state TEXT NOT NULL) WITHOUT ROWID"
        )
        before = eng.monitor.snapshot()
        batch: List[Tuple[str, str]] = []
        n = 0
        try:
            for entity_id, state in eng.iter_states(self.fixed_ast()):
                batch.append((entity_id, state))
                if len(batch) >= _DELTA_CHUNK:
                    self.conn.executemany(
                        "INSERT OR REPLACE INTO new_day (entity_id, state) "
                        "VALUES (?, ?)", batch)
                    n += len(batch)
                    batch.clear()
            if batch:
                self.conn.executemany(
                    "INSERT OR REPLACE INTO new_day (entity_id, state) "
                    "VALUES (?, ?)", batch)
                n += len(batch)
                batch.clear()
        finally:
            self._m.day_rows_extracted = n
        after = eng.monitor.snapshot()
        io = {
            "page_reads": after["page_reads"] - before["page_reads"],
            "bytes_read": after["bytes_read"] - before["bytes_read"],
            "posting_files_opened": (
                after["posting_files_opened"] - before["posting_files_opened"]
            ),
            "child_rows_processed": (
                after["child_rows_processed"] - before["child_rows_processed"]
            ),
        }
        return io

    # ------------------------------------------------------------------
    # 第二遍（单个写事务）：增量更新日状态、来源与区间
    # ------------------------------------------------------------------

    def _apply(
        self,
        date_text: str,
        date_ord: int,
        token: str,
        ast_fp: str,
        io: Dict[str, Any],
    ) -> RegisterResult:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            # 0) 在写事务内读取“该日是否已登记”，与后续增量同一快照。
            old_row = self.conn.execute(
                "SELECT index_token FROM day_source WHERE business_date = ?",
                (date_text,),
            ).fetchone()
            replaced = old_row is not None
            # 1) 在写事务内、基于已提交的旧日状态计算受影响对象。
            self._materialize_delta(date_text)
            self._m.delta_rows = int(self.conn.execute(
                "SELECT COUNT(*) FROM temp.delta").fetchone()[0])

            # 2) 用新结果整体覆盖该日状态（集合级，不装 Python 列表）。
            cur = self.conn.execute(
                "DELETE FROM day_state WHERE business_date = ?", (date_text,)
            )
            self._m.day_state_rows_deleted = max(cur.rowcount, 0)
            cur = self.conn.execute(
                "INSERT INTO day_state (business_date, date_ord, entity_id, state) "
                "SELECT ?, ?, entity_id, state FROM temp.new_day",
                (date_text, date_ord),
            )
            self._m.day_state_rows_inserted = max(cur.rowcount, 0)

            # 3) 只对受影响对象在其日期处调整区间（分块流式消费 delta）。
            adjustments = self._apply_delta_episodes(date_ord)

            # 4) 来源登记：同一天再次登记同一索引代号是幂等重送；新代号即使
            #    判定结果完全没变，也要更新该日的来源登记。
            self.conn.execute(
                "INSERT INTO day_source "
                "(business_date, date_ord, index_token, ast_fingerprint, "
                " registered_at, update_seq) "
                "VALUES (?, ?, ?, ?, ?, "
                "        (SELECT COALESCE(MAX(update_seq), 0) + 1 FROM day_source)) "
                "ON CONFLICT(business_date) DO UPDATE SET "
                "  index_token = excluded.index_token, "
                "  ast_fingerprint = excluded.ast_fingerprint, "
                "  registered_at = excluded.registered_at, "
                "  update_seq = excluded.update_seq",
                (date_text, date_ord, token, ast_fp, _iso_now()),
            )

            if self.before_commit_hook is not None:
                self.before_commit_hook()  # 故障注入：此处中断则整笔回滚

            seq_row = self.conn.execute(
                "SELECT MAX(update_seq) AS s FROM day_source"
            ).fetchone()
            commit_seq = int(seq_row["s"]) if seq_row["s"] is not None else 0
            self.conn.execute("COMMIT")
        except BaseException:
            # 任何异常（含注入的崩溃前异常）都回滚，绝不留下“新登记配旧区间”。
            self.conn.execute("ROLLBACK")
            raise
        finally:
            self.conn.execute("DROP TABLE IF EXISTS temp.delta")
            self.conn.execute("DROP TABLE IF EXISTS temp.new_day")

        return RegisterResult(
            business_date=date_text,
            index_token=token,
            replaced=replaced,
            changed_entities=self._m.delta_rows,
            episode_adjustments=adjustments,
            source_updated=True,
            commit_seq=commit_seq,
            metrics=self._m,
            io=io,
        )

    def _materialize_delta(self, date_text: str) -> None:
        """受影响对象 = 旧有今无 / 新旧状态不同 / 旧无今有。"""
        self.conn.execute("DROP TABLE IF EXISTS temp.delta")
        self.conn.execute(
            "CREATE TEMP TABLE delta ("
            "entity_id TEXT PRIMARY KEY, old_state TEXT, new_state TEXT"
            ") WITHOUT ROWID"
        )
        self.conn.execute(
            "INSERT INTO temp.delta (entity_id, old_state, new_state) "
            "SELECT o.entity_id, o.state, n.state "
            "FROM main.day_state o "
            "LEFT JOIN temp.new_day n ON n.entity_id = o.entity_id "
            "WHERE o.business_date = ? "
            "  AND (n.entity_id IS NULL OR n.state <> o.state)",
            (date_text,),
        )
        self.conn.execute(
            "INSERT INTO temp.delta (entity_id, old_state, new_state) "
            "SELECT n.entity_id, NULL, n.state "
            "FROM temp.new_day n "
            "LEFT JOIN main.day_state o "
            "  ON o.entity_id = n.entity_id AND o.business_date = ? "
            "WHERE o.entity_id IS NULL",
            (date_text,),
        )

    def _apply_delta_episodes(self, date_ord: int) -> int:
        adjustments = 0
        last = ""
        # 只统计“区间增量维护”这段 VM 步数，不把整日状态写入算进来。
        # 小增量时逐指令精确计数（granularity=1），大增量按 1000 步采样，
        # 避免高频回调拖慢批量更新；两种粒度都会记录。
        n_delta = max(self._m.delta_rows, 1)
        granularity = 1 if n_delta <= _VM_EXACT_ROWS else 1000
        self._vm_steps = 0
        self.conn.set_progress_handler(self._on_vm_step, granularity)
        try:
            while True:
                rows = self.conn.execute(
                    "SELECT entity_id, old_state, new_state FROM temp.delta "
                    "WHERE entity_id > ? ORDER BY entity_id LIMIT ?",
                    (last, _DELTA_CHUNK),
                ).fetchall()
                if not rows:
                    break
                for r in rows:
                    adjustments += self._adjust_episode(
                        r["entity_id"], date_ord, r["old_state"], r["new_state"]
                    )
                last = rows[-1]["entity_id"]
        finally:
            self.conn.set_progress_handler(None, 0)
            self._m.episode_vm_steps = self._vm_steps
            self._m.episode_vm_granularity = granularity
        return adjustments

    def _on_vm_step(self) -> int:
        self._vm_steps += 1
        return 0

    # ------------------------------------------------------------------
    # 区间增量维护（核心算法）
    # ------------------------------------------------------------------

    def _adjust_episode(
        self,
        eid: str,
        d: int,
        old_state: Optional[str],
        new_state: Optional[str],
    ) -> int:
        """对象 eid 在自然日 d 处的状态由 old_state 变为 new_state。

        ``None`` 表示该日“未登记该对象”。只按索引查找并调整 eid 在 d 处
        受影响的区间段（覆盖段及其左右**紧邻**段），不做全表重建，也不
        逐日扫描该对象的整条时间线。
        """
        if old_state == new_state:
            return 0
        n = 0
        # 1) 断开覆盖 d 的旧段：一分为二或截断。
        #    用 (entity_id, last_ord) 索引从 d 起取“结束日最早”的一段，
        #    单次索引探测即可定位覆盖段；不按 first_ord 从头扫描该对象的
        #    整条时间线。
        covering = self._episode_lookup(
            "SELECT first_ord, last_ord, state FROM episode_segment "
            "WHERE entity_id = ? AND last_ord >= ? "
            "ORDER BY last_ord LIMIT 1",
            (eid, d),
        )
        if covering is not None and int(covering["first_ord"]) > d:
            covering = None  # 该段起点在 d 之后：d 落在空档里
        if covering is not None:
            L, R, S = int(covering["first_ord"]), int(covering["last_ord"]), \
                covering["state"]
            if old_state is None or old_state != S:
                raise LedgerError(
                    f"区间表与日状态不一致: {eid!r} 在 {_ord_to_day(d)}"
                )
            self._delete_segment(eid, L)
            n += 1
            if L < d:
                self._insert_segment(eid, L, d - 1, S)
                n += 1
            if d < R:
                self._insert_segment(eid, d + 1, R, S)
                n += 1
        elif old_state is not None:
            raise LedgerError(
                f"区间表与日状态不一致: {eid!r} 在 {_ord_to_day(d)} 缺少区间"
            )

        # 2) 新状态非 None：在 d 插入单日段，并与左右同态紧邻段合并。
        if new_state is not None:
            n += self._insert_and_merge(eid, d, new_state)
        return n

    def _insert_and_merge(self, eid: str, d: int, state: str) -> int:
        n = 0
        lo = hi = d
        left = self._episode_lookup(
            "SELECT first_ord, last_ord, state FROM episode_segment "
            "INDEXED BY episode_segment_by_end "
            "WHERE entity_id = ? AND last_ord = ?",
            (eid, d - 1),
        )
        if left is not None and left["state"] == state:
            lo = int(left["first_ord"])
            self._delete_segment(eid, lo)
            n += 1
        right = self._episode_lookup(
            "SELECT first_ord, last_ord, state FROM episode_segment "
            "INDEXED BY sqlite_autoindex_episode_segment_1 "
            "WHERE entity_id = ? AND first_ord = ?",
            (eid, d + 1),
        )
        if right is not None and right["state"] == state:
            hi = int(right["last_ord"])
            self._delete_segment(eid, int(right["first_ord"]))
            n += 1
        self._insert_segment(eid, lo, hi, state)
        return n + 1

    def _episode_lookup(self, sql: str, args: Tuple[Any, ...]):
        self._m.episode_lookup_statements += 1
        row = self.conn.execute(sql, args).fetchone()
        if row is not None:
            self._m.episode_rows_read += 1
        return row

    def _insert_segment(self, eid: str, L: int, R: int, state: str) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO episode_segment "
            "(entity_id, first_ord, last_ord, first_day, last_day, state) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (eid, L, R, _ord_to_day(L), _ord_to_day(R), state),
        )
        self._m.episode_rows_inserted += 1

    def _delete_segment(self, eid: str, first_ord: int) -> None:
        self.conn.execute(
            "DELETE FROM episode_segment WHERE entity_id = ? AND first_ord = ?",
            (eid, first_ord),
        )
        self._m.episode_rows_deleted += 1

    # ------------------------------------------------------------------
    # 读接口
    # ------------------------------------------------------------------

    def episodes(
        self,
        entity_id: Optional[str] = None,
        *,
        first_day: Optional[str] = None,
        last_day: Optional[str] = None,
        state: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """读取区间。两端日期都包含；可按对象、日期窗、状态过滤。"""
        where: List[str] = []
        args: List[Any] = []
        if entity_id is not None:
            where.append("entity_id = ?")
            args.append(entity_id)
        if first_day is not None:
            _t, o = parse_business_date(first_day)
            where.append("last_ord >= ?")
            args.append(o)
        if last_day is not None:
            _t, o = parse_business_date(last_day)
            where.append("first_ord <= ?")
            args.append(o)
        if state is not None:
            if state not in _STATES:
                raise LedgerError(f"state 必须是 {_STATES} 之一")
            where.append("state = ?")
            args.append(state)
        sql = "SELECT entity_id, first_day, last_day, state FROM episode_segment"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY entity_id, first_ord"
        if limit is not None:
            if limit < 0 or offset < 0:
                raise LedgerError("limit/offset 必须非负")
            sql += " LIMIT ? OFFSET ?"
            args.extend([limit, offset])
        return [dict(r) for r in self.conn.execute(sql, args)]

    def daily_sources(
        self,
        *,
        business_date: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        index_token: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """读取按日来源登记表。下游据此区分缺报与对象不在日报里。"""
        where: List[str] = []
        args: List[Any] = []
        if business_date is not None:
            parse_business_date(business_date)
            where.append("business_date = ?")
            args.append(business_date)
        if start is not None:
            _t, o = parse_business_date(start)
            where.append("date_ord >= ?")
            args.append(o)
        if end is not None:
            _t, o = parse_business_date(end)
            where.append("date_ord <= ?")
            args.append(o)
        if index_token is not None:
            where.append("index_token = ?")
            args.append(index_token)
        sql = (
            "SELECT business_date, index_token, ast_fingerprint, registered_at, "
            "update_seq FROM day_source"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY date_ord"
        if limit is not None:
            if limit < 0 or offset < 0:
                raise LedgerError("limit/offset 必须非负")
            sql += " LIMIT ? OFFSET ?"
            args.extend([limit, offset])
        return [dict(r) for r in self.conn.execute(sql, args)]

    def object_days(
        self,
        entity_id: str,
        *,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """某个对象在区间内的逐日状态（用于核对/调试）。"""
        where = ["entity_id = ?"]
        args: List[Any] = [entity_id]
        if start is not None:
            _t, o = parse_business_date(start)
            where.append("date_ord >= ?")
            args.append(o)
        if end is not None:
            _t, o = parse_business_date(end)
            where.append("date_ord <= ?")
            args.append(o)
        sql = (
            "SELECT business_date, state FROM day_state WHERE "
            + " AND ".join(where)
            + " ORDER BY date_ord"
        )
        return [dict(r) for r in self.conn.execute(sql, args)]

    def stats(self) -> Dict[str, Any]:
        def one(sql: str):
            r = self.conn.execute(sql).fetchone()
            return None if r is None else r[0]

        return {
            "days_registered": one("SELECT COUNT(*) FROM day_source"),
            "distinct_index_tokens": one(
                "SELECT COUNT(DISTINCT index_token) FROM day_source"),
            "day_state_rows": one("SELECT COUNT(*) FROM day_state"),
            "episode_rows": one("SELECT COUNT(*) FROM episode_segment"),
            "first_day": one("SELECT MIN(business_date) FROM day_source"),
            "last_day": one("SELECT MAX(business_date) FROM day_source"),
        }
