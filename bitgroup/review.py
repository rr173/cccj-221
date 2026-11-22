"""现场复核包导出：三态抽样、整组预算与可原子发布的目录交付。

解决的问题
==========

现场抽检只有有限的明细处理额度。审核员要从一个已建索引里按一套规则挑
一批主对象，并把**中选对象的全部子记录**带走离线复核。现有查询能列对象
ID、也能解释单条判定，但按对象数翻页会带来两个问题：

* 前几个对象可能全部来自同一判定类；
* 按对象数截断会把一组原始检测明细截成两半。

本模块按“先整组算成本、再决定是否中选”的口径挑人：一个对象只有在
**所在类还有名额**且**它的全部子记录都装得进剩余额度**时才中选。未中选
不消耗任何额度，也不阻止后面的对象中选；无子记录的对象成本为零，额度
用完后仍可入选。

抽样口径
========

* **判定**：沿用现有三态语义（:class:`bitgroup.engine.Engine`）；名额为零
  的类别的对象照常归类、参与优先级，只是不会被选中——绝不因名额为零而
  改变判定。
* **优先级**：只由
  ``SHA-256(UTF-8(seed) + 0x00 + UTF-8(entity_id))`` 的摘要字节升序决定；
  摘要相同则比较 ``entity_id`` 的 UTF-8 字节。导入行序、索引局部编号和
  子表顺序都不参与优先级。
* **逻辑顺序**：按上述优先级从头到尾考察对象；中选消耗一个本类名额和
  相应的子记录额度，未中选不消耗，也不阻止后面的对象中选。不在类间挪
  名额，不要求填满额度，也不求“装入数量最多”的组合。
* **成本**：对象的全部子记录数（``child_range`` 长度），与规则是否满足
  无关；中选对象导出其**全部**子记录，包括不满足规则的子记录。

交付物（一个目录，三份文件）
============================

``parents.jsonl``
    按中选优先级逐行写主对象：``entity_id``、判定 ``state``、以及保留索引
    表声明中全部属性的 ``attributes``（业务值，非字典编号/位号）。
``children.jsonl``
    按相同主对象顺序分组，组内按 ``child_id`` 的 UTF-8 字节升序；每行
    ``entity_id``、``child_id`` 与子表 ``attributes``。每个中选对象的所有
    子记录恰好导出一次；落选对象不出现在任一文件。
``manifest.json``
    索引代号、规范化规则指纹、seed、名额与子记录上限、三个状态各自的
    候选数与中选数，以及实际导出的子记录总数。

空包同样产出三份文件（两份 JSONL 为空）。

发布与失败
==========

单个输出路径只考虑一个发布者：目标路径已存在（文件、目录或符号链接）
一律拒绝，不覆盖。导出先写到目标同级的隐藏暂存目录，三份文件都写完并
``fsync`` 后才 ``rename`` 成最终目录；任何一步失败都会清理暂存，最终
目录不会留下可被当成成品的半包。

内存与 I/O
==========

* 考察阶段按优先级流式归并：先把每个主对象的优先级键、判定与组大小写
  磁盘暂存，再按 ``(digest, entity_id)`` 顺序读取——不把全体候选、中选
  名单或一个大子组整体装进 Python 内存。名额和单组大小都随数据增长。
* 规则求值本身需要读取 posting/子行，这是判定成本；**导出阶段**只恢复
  中选对象及其子组的属性，落选大组不会被展开或逐条恢复属性。
* 单组内按 ``child_id`` 排序时，超过块上限的组走“分块排序 + 多路归并”，
  不把整个大组读入内存。

本命令只读索引：没有原始 JSONL、日报台账或额外服务。旧索引与已有查询
入口不受影响。
"""

from __future__ import annotations

import hashlib
import heapq
import json
import os
import shutil
import struct
import tempfile
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from .engine import Engine
from .errors import ReviewError

STATES = ("true", "false", "unknown")

PARENTS_NAME = "parents.jsonl"
CHILDREN_NAME = "children.jsonl"
MANIFEST_NAME = "manifest.json"

# 优先级键暂存记录（变长）：
#   <32s B H Q Q> = 完整 SHA-256 摘要, 判定码, entity_id 长度, 子记录数, pid
#   随后是 entity_id 的 UTF-8 字节
# 完整摘要入盘，归并时的比较就是题目要求的“摘要字节升序，摘要相同再比
# entity_id 的 UTF-8 字节”，不引入局部编号。
_KEY_HEAD = struct.Struct("<32sBIQQ")
_STATE_CODE = {"true": 0, "false": 1, "unknown": 2}
_CODE_STATE = {v: k for k, v in _STATE_CODE.items()}

# 中选名单暂存记录：<Q B Q> = pid, 判定码, 子记录数；随后 <H 长度> + entity_id
_SEL_HEAD = struct.Struct("<QBQI")  # pid, 判定码, 子记录数, entity_id 长度
_RUN_HEAD = struct.Struct("<IQ")    # child_id 长度, cid


# --------------------------------------------------------------------------
# 优先级
# --------------------------------------------------------------------------


def priority_digest(seed: str, entity_id: str) -> bytes:
    """``SHA-256(UTF-8(seed) + 0x00 + UTF-8(entity_id))`` 的 32 字节摘要。"""
    h = hashlib.sha256()
    h.update(seed.encode("utf-8"))
    h.update(b"\x00")
    h.update(entity_id.encode("utf-8"))
    return h.digest()


def priority_key(seed: str, entity_id: str) -> Tuple[bytes, bytes]:
    """排序键：先摘要字节，再 ``entity_id`` 的 UTF-8 字节。"""
    return priority_digest(seed, entity_id), entity_id.encode("utf-8")


# --------------------------------------------------------------------------
# 结果对象
# --------------------------------------------------------------------------


@dataclass
class ReviewExport:
    out_dir: str
    manifest: Dict[str, Any]
    metrics: Dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------
# 磁盘暂存：优先级键按 (digest, entity_id) 外部排序
# --------------------------------------------------------------------------


class _KeySpool:
    """把优先级键分块排序后多路归并，内存占用只与块上限有关。"""

    _BLOCK_BYTES = 2 * 1024 * 1024

    def __init__(self, scratch_dir: str, seed: str):
        self.scratch_dir = scratch_dir
        self.seed_prefix = seed.encode("utf-8") + b"\x00"
        self._path = os.path.join(scratch_dir, "keys.bin")
        self._fh = open(self._path, "w+b", buffering=1 << 20)
        self._cur = 0
        self._count = 0
        self._blocks: List[str] = []
        self.max_batch_entries = 0
        self.blocks_written = 0
        self.records_written = 0
        self._finished = False

    # ---- 写入 ----------------------------------------------------------

    def add(self, entity_id: str, state: str, pid: int, n_children: int) -> None:
        h = hashlib.sha256()
        h.update(self.seed_prefix)
        h.update(entity_id.encode("utf-8"))
        digest = h.digest()
        if self._cur >= self._BLOCK_BYTES:
            self._flush_block()
        b = entity_id.encode("utf-8")
        self._fh.write(_KEY_HEAD.pack(
            digest, _STATE_CODE[state], len(b), n_children, pid))
        self._fh.write(b)
        self._cur += _KEY_HEAD.size + len(b)
        self._count += 1
        self.records_written += 1

    def _flush_block(self) -> None:
        if self._count == 0:
            return
        self._fh.flush()
        items: List[Tuple[bytes, bytes, int, int, int]] = []
        with open(self._path, "rb") as fh:
            while True:
                head = fh.read(_KEY_HEAD.size)
                if len(head) < _KEY_HEAD.size:
                    break
                digest, code, ln, n, pid = _KEY_HEAD.unpack(head)
                ident = fh.read(ln)
                items.append((digest, ident, code, n, pid))
        items.sort(key=lambda t: (t[0], t[1]))
        self.max_batch_entries = max(self.max_batch_entries, len(items))
        path = os.path.join(self.scratch_dir,
                            f"block_{len(self._blocks):04d}.bin")
        with open(path, "wb", buffering=1 << 20) as out:
            for digest, ident, code, n, pid in items:
                out.write(_KEY_HEAD.pack(digest, code, len(ident), n, pid))
                out.write(ident)
        self._blocks.append(path)
        self.blocks_written += 1
        self._fh.seek(0)
        self._fh.truncate(0)
        self._cur = 0
        self._count = 0
        del items

    def finish_writes(self) -> None:
        if self._finished:
            return
        self._flush_block()
        self._fh.close()
        self._finished = True

    # ---- 读取 ----------------------------------------------------------

    @staticmethod
    def _iter_block(path: str):
        with open(path, "rb", buffering=1 << 20) as fh:
            while True:
                head = fh.read(_KEY_HEAD.size)
                if len(head) < _KEY_HEAD.size:
                    return
                digest, code, ln, n, pid = _KEY_HEAD.unpack(head)
                ident = fh.read(ln)
                yield digest, ident, code, n, pid

    def iter_ordered(self) -> Iterator[Tuple[bytes, str, str, int, int]]:
        """产出 ``(digest, entity_id, state, n_children, pid)``，升序。"""
        streams = [self._iter_block(p) for p in self._blocks]
        heap: List[Tuple[bytes, bytes, int, int, int, int]] = []
        for si, it in enumerate(streams):
            rec = next(it, None)
            if rec is not None:
                digest, ident, code, n, pid = rec
                heapq.heappush(heap, (digest, ident, si, n, code, pid))
        while heap:
            digest, ident, si, n, code, pid = heapq.heappop(heap)
            yield digest, ident.decode("utf-8"), _CODE_STATE[code], n, pid
            nxt = next(streams[si], None)
            if nxt is not None:
                d2, i2, c2, n2, p2 = nxt
                heapq.heappush(heap, (d2, i2, si, n2, c2, p2))

    def close(self) -> None:
        if not self._finished:
            try:
                self._fh.close()
            finally:
                self._finished = True


class _SelectedSpool:
    """中选名单按优先级顺序追加到磁盘，导出阶段顺序读回。"""

    def __init__(self, scratch_dir: str):
        self.path = os.path.join(scratch_dir, "selected.bin")
        self._fh = open(self.path, "w+b", buffering=1 << 20)
        self.count = 0
        self._finished = False

    def add(self, pid: int, state: str, n_children: int, entity_id: str) -> None:
        b = entity_id.encode("utf-8")
        self._fh.write(_SEL_HEAD.pack(pid, _STATE_CODE[state], n_children,
                                      len(b)))
        self._fh.write(b)
        self.count += 1

    def finish(self) -> None:
        if self._finished:
            return
        self._fh.flush()
        self._fh.close()
        self._finished = True

    def iter_all(self):
        with open(self.path, "rb", buffering=1 << 20) as fh:
            while True:
                head = fh.read(_SEL_HEAD.size)
                if len(head) < _SEL_HEAD.size:
                    return
                pid, code, n, ln = _SEL_HEAD.unpack(head)
                ident = fh.read(ln).decode("utf-8")
                yield pid, _CODE_STATE[code], n, ident


# --------------------------------------------------------------------------
# 单组内 child_id 排序（超过块上限走分块排序 + 多路归并）
# --------------------------------------------------------------------------


_GROUP_CHUNK_ENTRIES = 8192


class _GroupSorter:
    """按 child_id UTF-8 字节升序产出 ``(child_id, cid)``，内存有界。"""

    def __init__(
        self,
        engine: Engine,
        scratch_dir: str,
        *,
        chunk_entries: int = _GROUP_CHUNK_ENTRIES,
    ):
        self.engine = engine
        self.scratch_dir = scratch_dir
        self.chunk_entries = max(1, chunk_entries)
        self.max_chunk_entries = 0
        self.runs_written = 0
        self._run_seq = 0

    def iter_sorted(self, first_cid: int, last_cid: int):
        size = last_cid - first_cid
        if size <= self.chunk_entries:
            self.max_chunk_entries = max(self.max_chunk_entries, max(0, size))
            buf = [(cid_id, cid)
                   for cid, cid_id in zip(range(first_cid, last_cid),
                                          self._read_ids(first_cid, last_cid))]
            buf.sort(key=lambda t: (t[0].encode("utf-8"), t[1]))
            yield from buf
            return
        runs: List[str] = []
        cid = first_cid
        while cid < last_cid:
            stop = min(cid + self.chunk_entries, last_cid)
            buf = [(cid_id, c)
                   for c, cid_id in zip(range(cid, stop),
                                        self._read_ids(cid, stop))]
            buf.sort(key=lambda t: (t[0].encode("utf-8"), t[1]))
            self.max_chunk_entries = max(self.max_chunk_entries, len(buf))
            path = os.path.join(self.scratch_dir,
                                f"group_run_{self._run_seq:05d}.bin")
            self._run_seq += 1
            with open(path, "wb", buffering=1 << 16) as fh:
                for cid_id, c in buf:
                    b = cid_id.encode("utf-8")
                    fh.write(_RUN_HEAD.pack(len(b), c))
                    fh.write(b)
            runs.append(path)
            self.runs_written += 1
            del buf
            cid = stop
        try:
            yield from self._merge_runs(runs)
        finally:
            for p in runs:
                try:
                    os.remove(p)
                except OSError:
                    pass

    def _read_ids(self, first_cid: int, last_cid: int):
        return self.engine.ident.iter_child_ids(first_cid, last_cid)

    @staticmethod
    def _iter_run(path: str):
        with open(path, "rb", buffering=1 << 16) as fh:
            while True:
                head = fh.read(_RUN_HEAD.size)
                if len(head) < _RUN_HEAD.size:
                    return
                ln, cid = _RUN_HEAD.unpack(head)
                yield fh.read(ln).decode("utf-8"), cid

    @staticmethod
    def _merge_runs(runs: List[str]):
        streams = [_GroupSorter._iter_run(p) for p in runs]
        heap: List[Tuple[bytes, int, int, str]] = []
        for si, it in enumerate(streams):
            rec = next(it, None)
            if rec is not None:
                heapq.heappush(heap, (rec[0].encode("utf-8"), rec[1], si,
                                      rec[0]))
        while heap:
            _ident, cid, si, text = heapq.heappop(heap)
            yield text, cid
            nxt = next(streams[si], None)
            if nxt is not None:
                heapq.heappush(heap, (nxt[0].encode("utf-8"), nxt[1], si,
                                      nxt[0]))


# --------------------------------------------------------------------------
# 导出器
# --------------------------------------------------------------------------


def _check_nonneg(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ReviewError(f"{name} 必须是非负整数，得到 {value!r}")
    return int(value)


def _reject_existing(path: str) -> None:
    if os.path.lexists(path):
        raise ReviewError(f"输出路径已存在，拒绝覆盖: {path}")


def _fsync_file(fh) -> None:
    fh.flush()
    os.fsync(fh.fileno())


def _tracemalloc_available() -> bool:
    try:
        import tracemalloc  # noqa: F401
    except ImportError:  # pragma: no cover - 标准库缺失时降级
        return False
    return True


def _peak_kib() -> Optional[int]:
    if not _tracemalloc_available():
        return None
    import tracemalloc

    _cur, peak = tracemalloc.get_traced_memory()
    return peak // 1024


class _Exporter:
    def __init__(
        self,
        engine: Engine,
        ast: Dict[str, Any],
        *,
        seed: str,
        quotas: Dict[str, int],
        max_children: int,
        out_dir: str,
        scratch_dir: Optional[str],
        before_publish: Optional[Callable[[], None]],
    ):
        self.eng = engine
        self.ast = ast
        self.seed = seed
        self.quotas = quotas
        self.max_children = max_children
        self.out_dir = os.path.abspath(out_dir)
        self.scratch_parent = scratch_dir
        self.before_publish = before_publish
        self.node, self.fp = engine._prepare(ast)
        self.metrics: Dict[str, Any] = {
            "phases": {},
            "candidates": {s: 0 for s in STATES},
            "selected": {s: 0 for s in STATES},
            "key_spool_records": 0,
            "key_spool_blocks": 0,
            "key_block_max_entries": 0,
            "selection_scanned": 0,
            "selection_short_circuited": False,
            "quota_exhausted_skips": {s: 0 for s in STATES},
            "child_budget_skips": 0,
            "export_parent_attr_reads": 0,
            "export_child_ids_read": 0,
            "export_child_attr_reads": 0,
            "export_groups": 0,
            "export_largest_group": 0,
            "export_group_chunk_max_entries": 0,
            "export_group_sort_runs": 0,
            "export_peak_kib": None,
        }

    # ---- 生命周期 ------------------------------------------------------

    def run(self) -> ReviewExport:
        parent = os.path.dirname(self.out_dir) or os.getcwd()
        if not os.path.isdir(parent):
            raise ReviewError(f"输出目录的上级不存在: {parent}")
        if self.scratch_parent is not None and not os.path.isdir(
                self.scratch_parent):
            raise ReviewError(f"暂存目录不存在: {self.scratch_parent}")
        _reject_existing(self.out_dir)
        scratch = tempfile.mkdtemp(prefix="bitgroup_review_",
                                   dir=self.scratch_parent)
        stage: Optional[str] = None
        spool = _KeySpool(scratch, self.seed)
        selected = _SelectedSpool(scratch)
        try:
            candidates = self._consider(spool)
            selected_counts = self._select(spool, selected)
            stage = tempfile.mkdtemp(
                prefix=".bitgroup_review_stage_", dir=parent)
            n_parents_out, n_children_out = self._export(selected, scratch,
                                                         stage)
            manifest = {
                "index_token": self.eng.index_token,
                "ast_fingerprint": self.fp,
                "seed": self.seed,
                "quotas": dict(self.quotas),
                "max_children": self.max_children,
                "candidates": dict(candidates),
                "selected": dict(selected_counts),
                "n_parents": n_parents_out,
                "n_children": n_children_out,
            }
            self._write_manifest(stage, manifest)
            if self.before_publish is not None:
                self.before_publish()
            _reject_existing(self.out_dir)
            os.rename(stage, self.out_dir)
            stage = None
            self._fsync_dir(parent)
            self.metrics["out_dir"] = self.out_dir
            return ReviewExport(self.out_dir, manifest, self.metrics)
        finally:
            spool.close()
            selected.finish()
            if stage is not None:
                shutil.rmtree(stage, ignore_errors=True)
            shutil.rmtree(scratch, ignore_errors=True)

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

    # ---- 第一遍：流式归类 + 优先级键落盘 ------------------------------

    def _consider(self, spool: _KeySpool) -> Dict[str, int]:
        mon = self.eng.monitor
        mon.reset_peaks()
        before = mon.snapshot()
        candidates = {s: 0 for s in STATES}
        ranges = self.eng.ident.ranges.iter_all()
        n = 0
        for pid, (entity_id, state) in enumerate(
                self.eng.iter_states(self.ast)):
            _p, start, end = next(ranges)
            if _p != pid:
                raise ReviewError("子组范围表与父身份流不一致")
            n_children = end - start
            candidates[state] += 1
            spool.add(entity_id, state, pid, n_children)
            n += 1
        if n != self.eng.n_parents:
            raise ReviewError("父身份流长度与索引声明不一致")
        spool.finish_writes()
        self.metrics["phases"]["consider"] = mon.delta(before)
        self.metrics["key_spool_records"] = spool.records_written
        self.metrics["key_spool_blocks"] = spool.blocks_written
        self.metrics["key_block_max_entries"] = spool.max_batch_entries
        self.metrics["candidates"] = dict(candidates)
        return candidates

    # ---- 第二遍：按优先级贪心选人（中选名单落盘） ----------------------

    def _select(self, spool: _KeySpool,
                selected: _SelectedSpool) -> Dict[str, int]:
        remaining = dict(self.quotas)
        remaining_children = self.max_children
        selected_counts = {s: 0 for s in STATES}
        try:
            if any(remaining.values()):
                for _d, entity_id, state, n_children, pid \
                        in spool.iter_ordered():
                    self.metrics["selection_scanned"] += 1
                    if remaining[state] <= 0:
                        self.metrics["quota_exhausted_skips"][state] += 1
                        continue
                    if n_children > remaining_children:
                        self.metrics["child_budget_skips"] += 1
                        continue
                    remaining[state] -= 1
                    remaining_children -= n_children
                    selected_counts[state] += 1
                    selected.add(pid, state, n_children, entity_id)
                    if not any(remaining.values()):
                        self.metrics["selection_short_circuited"] = True
                        break
        finally:
            selected.finish()
        self.metrics["selected"] = dict(selected_counts)
        self.metrics["remaining_child_budget"] = remaining_children
        return selected_counts

    # ---- 第三遍：只恢复中选对象及其子组并写正式文件 --------------------

    def _export(self, selected: _SelectedSpool, scratch: str,
                stage: str) -> Tuple[int, int]:
        mon = self.eng.monitor
        mon.reset_peaks()
        before = mon.snapshot()
        started_tracing = False
        if _tracemalloc_available():
            import tracemalloc
            if not tracemalloc.is_tracing():
                tracemalloc.start()
                started_tracing = True
            tracemalloc.reset_peak()

        child_attrs = list(self.eng.child_schema.items())
        parent_attrs = list(self.eng.parent_schema.items())
        sorter = _GroupSorter(self.eng, scratch)

        parents_path = os.path.join(stage, PARENTS_NAME)
        children_path = os.path.join(stage, CHILDREN_NAME)
        n_parents = n_children = 0
        pfh = open(parents_path, "w", encoding="utf-8", buffering=1 << 20)
        cfh = open(children_path, "w", encoding="utf-8", buffering=1 << 20)
        try:
            for pid, state, n_children_expected, entity_id \
                    in selected.iter_all():
                attributes = {}
                for attr, _typ in parent_attrs:
                    attributes[attr] = self.eng._get_col_reader(
                        "parent", attr).read(pid)
                    self.metrics["export_parent_attr_reads"] += 1
                pfh.write(json.dumps({
                    "entity_id": entity_id,
                    "state": state,
                    "attributes": attributes,
                }, ensure_ascii=False, separators=(",", ":")))
                pfh.write("\n")
                n_parents += 1

                start, end = self.eng.ident.child_range(pid)
                if end - start != n_children_expected:
                    raise ReviewError(
                        f"子记录数与判定阶段不一致: {entity_id!r}")
                self.metrics["export_groups"] += 1
                self.metrics["export_largest_group"] = max(
                    self.metrics["export_largest_group"], end - start)
                for child_id, cid in sorter.iter_sorted(start, end):
                    self.metrics["export_child_ids_read"] += 1
                    cattrs = {}
                    for attr, _typ in child_attrs:
                        cattrs[attr] = self.eng._get_col_reader(
                            "child", attr).read(cid)
                        self.metrics["export_child_attr_reads"] += 1
                    cfh.write(json.dumps({
                        "entity_id": entity_id,
                        "child_id": child_id,
                        "attributes": cattrs,
                    }, ensure_ascii=False, separators=(",", ":")))
                    cfh.write("\n")
                    n_children += 1
            _fsync_file(pfh)
            _fsync_file(cfh)
        finally:
            pfh.close()
            cfh.close()

        self.metrics["export_group_chunk_max_entries"] = \
            sorter.max_chunk_entries
        self.metrics["export_group_sort_runs"] = sorter.runs_written
        if started_tracing:
            self.metrics["export_peak_kib"] = _peak_kib()
            import tracemalloc
            tracemalloc.stop()
        self.metrics["phases"]["export"] = mon.delta(before)
        return n_parents, n_children

    # ---- manifest ------------------------------------------------------

    @staticmethod
    def _write_manifest(stage: str, manifest: Dict[str, Any]) -> None:
        path = os.path.join(stage, MANIFEST_NAME)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, ensure_ascii=False, indent=2,
                      sort_keys=True)
            fh.write("\n")
            _fsync_file(fh)


# --------------------------------------------------------------------------
# 公开入口
# --------------------------------------------------------------------------


def export_review_package(
    index_dir: str,
    ast: Dict[str, Any],
    *,
    seed: str,
    quota_true: int,
    quota_false: int,
    quota_unknown: int,
    max_children: int,
    out_dir: str,
    cache_budget_bytes: int = 32 * 1024 * 1024,
    scratch_dir: Optional[str] = None,
    before_publish: Optional[Callable[[], None]] = None,
) -> ReviewExport:
    """导出一个复核包到尚不存在的 ``out_dir``（失败不留半包）。

    ``before_publish`` 仅供故障注入测试：在三份文件写完、``rename`` 之前
    调用；抛异常即验证“发布前写入失败不会留下成品目录”。
    """
    if not isinstance(seed, str):
        raise ReviewError(f"seed 必须是字符串，得到 {seed!r}")
    quotas = {
        "true": _check_nonneg("quota_true", quota_true),
        "false": _check_nonneg("quota_false", quota_false),
        "unknown": _check_nonneg("quota_unknown", quota_unknown),
    }
    max_children = _check_nonneg("max_children", max_children)
    if not isinstance(out_dir, str) or not out_dir:
        raise ReviewError("out_dir 必须是非空路径")
    _reject_existing(out_dir)
    with Engine(index_dir, cache_budget_bytes=cache_budget_bytes) as eng:
        exp = _Exporter(
            eng, ast,
            seed=seed,
            quotas=quotas,
            max_children=max_children,
            out_dir=out_dir,
            scratch_dir=scratch_dir,
            before_publish=before_publish,
        )
        return exp.run()


# --------------------------------------------------------------------------
# 供脚本/测试使用的独立参考实现（不接触索引与引擎）
# --------------------------------------------------------------------------


def plan_from_rows(
    parents: List[Dict[str, Any]],
    children_by_entity: Dict[str, List[Dict[str, Any]]],
    states: Dict[str, str],
    *,
    seed: str,
    quotas: Dict[str, int],
    max_children: int,
) -> Dict[str, Any]:
    """直接从原始小样本计算中选名单与整组成本（独立对照用）。

    只使用 Python 原生排序与循环，不复用引擎、暂存或导出逻辑。
    """
    keys = []
    for p in parents:
        eid = p["entity_id"]
        keys.append((priority_key(seed, eid), eid))
    keys.sort(key=lambda kv: kv[0])
    remaining = dict(quotas)
    remaining_children = max_children
    selected: List[Dict[str, Any]] = []
    considered = []
    for _k, eid in keys:
        state = states[eid]
        cost = len(children_by_entity.get(eid, []))
        considered.append({"entity_id": eid, "state": state, "cost": cost})
        if remaining[state] <= 0:
            continue
        if cost > remaining_children:
            continue
        remaining[state] -= 1
        remaining_children -= cost
        selected.append({"entity_id": eid, "state": state, "cost": cost})
    return {
        "considered": considered,
        "selected": selected,
        "selected_counts": {
            s: sum(1 for r in selected if r["state"] == s) for s in STATES
        },
        "total_children": sum(r["cost"] for r in selected),
        "remaining_children": remaining_children,
    }
