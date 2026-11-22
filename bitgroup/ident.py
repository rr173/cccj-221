"""主/子身份与位图局部编号的可逆映射。

磁盘布局（全部在索引目录下）::

    ident/
      parent.hash        父 entity_id -> pid 的开放寻址定长槽目录
      parent.blob        pid 顺序的 entity_id 字符串
      parent.idx         blob 稀疏偏移采样
      child.hash         child_id -> cid
      child.blob         cid 顺序的 child_id 字符串
      child.idx
      child_range.bin    每个 pid 一条 (start_cid, len)，定长 16 字节

哈希目录用 pwrite 逐槽构建、pread 单槽查询，不把目录整体映射进内存，
因此在 128 MiB 预算下也能支撑千万级身份。槽空标志用独立的空/墓碑标记
（hash 字段为 0 表示空，正常键哈希为 0 时改写为 1）。
"""

from __future__ import annotations

import mmap
import os
import struct
from typing import BinaryIO, List, Optional, Sequence, Tuple

from .errors import InputDataError

SLOT = struct.Struct("<QIQQ")  # hash64, local_id, blob_offset, blob_len
_EMPTY_HASH = 0
_GUARD_HASH = 1  # 真实哈希为 0 时改写
_TOMB_LOCAL = 0xFFFFFFFFFFFFFFFF
_IDX_SAMPLE = 1024
_RANGE = struct.Struct("<QQ")


def fnv1a64(data: bytes) -> int:
    h = 0xCBF29CE484222325
    for b in data:
        h ^= b
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    if h == _EMPTY_HASH:
        h = _GUARD_HASH
    return h


class HashDirectory:
    """开放寻址定长槽哈希目录（线性探测）。"""

    def __init__(self, path: str, n_buckets: int):
        self.path = path
        self.n_buckets = n_buckets
        self._fh: Optional[BinaryIO] = None

    # ---- 构建期 --------------------------------------------------------

    def init_writer(self) -> BinaryIO:
        # 稀疏文件：seek 到末位写 0 即得到全零（空槽）目录
        fh = open(self.path, "w+b")
        fh.seek(self.n_buckets * SLOT.size - 1)
        fh.write(b"\x00")
        fh.flush()
        self._fh = fh
        return fh

    def insert(
        self,
        fh: BinaryIO,
        key: str,
        local_id: int,
        blob_offset: int,
        blob_len: int,
    ) -> None:
        kb = key.encode("utf-8")
        h = fnv1a64(kb)
        pos = h % self.n_buckets
        for _ in range(self.n_buckets):
            fh.seek(pos * SLOT.size)
            raw = fh.read(SLOT.size)
            hh, lid, _off, _len = SLOT.unpack(raw)
            if hh == _EMPTY_HASH:
                fh.seek(pos * SLOT.size)
                fh.write(SLOT.pack(h, local_id, blob_offset, blob_len))
                return
            if hh == h and lid != _TOMB_LOCAL:
                raise InputDataError(f"重复身份: {key!r}")
            pos = (pos + 1) % self.n_buckets
        raise InputDataError("哈希目录已满（负载因子设置异常）")

    # ---- 查询期 --------------------------------------------------------

    def open(self) -> None:
        if self._fh is None:
            self._fh = open(self.path, "rb")

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def lookup_hash(self, h: int) -> Optional[Tuple[int, int, int]]:
        """按已算好的哈希探测，返回 (local_id, blob_offset, blob_len)。"""
        self.open()
        assert self._fh is not None
        pos = h % self.n_buckets
        fd = self._fh.fileno()
        for _ in range(self.n_buckets):
            raw = os.pread(fd, SLOT.size, pos * SLOT.size)
            hh, lid, off, blen = SLOT.unpack(raw)
            if hh == _EMPTY_HASH:
                return None
            if hh == h and lid != _TOMB_LOCAL:
                return lid, off, blen
            pos = (pos + 1) % self.n_buckets
        return None

    def lookup(self, key: str) -> Optional[Tuple[int, int, int]]:
        return self.lookup_hash(fnv1a64(key.encode("utf-8")))

    def read_key(self, off: int, blen: int) -> str:
        assert self._fh is not None
        return os.pread(self._fh.fileno(), blen, off).decode("utf-8")


class TextHashDirectory:
    """16 字节定长槽的文本字典哈希目录：(hash64, u32 dict_id, u32 file_id)。

    与身份目录（28 字节槽）分开，专用于 text 列字典与 text posting 取值索引。
    """

    SLOT = struct.Struct("<QII")

    def __init__(self, path: str, n_buckets: int):
        self.path = path
        self.n_buckets = n_buckets
        self._fh: Optional[BinaryIO] = None

    def init_writer(self) -> BinaryIO:
        fh = open(self.path, "w+b")
        fh.seek(self.n_buckets * self.SLOT.size - 1)
        fh.write(b"\x00")
        fh.flush()
        return fh

    def insert(self, fh: BinaryIO, key: str, dict_id: int, file_id: int) -> None:
        h = fnv1a64(key.encode("utf-8"))
        pos = h % self.n_buckets
        for _ in range(self.n_buckets):
            fh.seek(pos * self.SLOT.size)
            hh, _d1, _d2 = self.SLOT.unpack(fh.read(self.SLOT.size))
            if hh == 0:
                fh.seek(pos * self.SLOT.size)
                fh.write(self.SLOT.pack(h, dict_id, file_id))
                return
            if hh == h:
                raise InputDataError(f"text 字典重复键: {key!r}")
            pos = (pos + 1) % self.n_buckets
        raise InputDataError("text 字典哈希目录已满")

    def open(self) -> None:
        if self._fh is None:
            self._fh = open(self.path, "rb")

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def lookup_hash(self, h: int) -> Optional[Tuple[int, int, int]]:
        self.open()
        assert self._fh is not None
        pos = h % self.n_buckets
        fd = self._fh.fileno()
        for _ in range(self.n_buckets):
            raw = os.pread(fd, self.SLOT.size, pos * self.SLOT.size)
            hh, d1, d2 = self.SLOT.unpack(raw)
            if hh == 0:
                return None
            if hh == h:
                return hh, d1, d2
            pos = (pos + 1) % self.n_buckets
        return None

    def lookup(self, key: str) -> Optional[Tuple[int, int, int]]:
        return self.lookup_hash(fnv1a64(key.encode("utf-8")))


class _IdBlob:
    """local_id 顺序的字符串 blob + 稀疏偏移采样，支持 id_at(local)。"""

    def __init__(self, blob_path: str, idx_path: str):
        self.blob_path = blob_path
        self.idx_path = idx_path
        self._fh: Optional[BinaryIO] = None
        self._index: List[Tuple[int, int]] = []  # (local_id, blob_offset)

    def build(self, ids: Sequence[str]) -> None:
        offsets: List[Tuple[int, int]] = []
        with open(self.blob_path, "wb") as blob:
            cur = 0
            for i, s in enumerate(ids):
                if i % _IDX_SAMPLE == 0:
                    offsets.append((i, cur))
                b = s.encode("utf-8")
                blob.write(b)
                cur += len(b)
        with open(self.idx_path, "wb") as idx:
            for lid, off in offsets:
                idx.write(struct.pack("<QQ", lid, off))

    def open(self) -> None:
        if self._fh is None:
            self._fh = open(self.blob_path, "rb")
            with open(self.idx_path, "rb") as idx:
                data = idx.read()
            self._index = [
                struct.unpack_from("<QQ", data, i)
                for i in range(0, len(data), 16)
            ]

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def id_at(self, local_id: int) -> str:
        self.open()
        assert self._fh is not None
        # 二分采样点
        lo, hi = 0, len(self._index) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if self._index[mid][0] <= local_id:
                lo = mid
            else:
                hi = mid - 1
        start_local, start_off = self._index[lo]
        fd = self._fh.fileno()
        off = start_off
        cur = start_local
        # 顺序扫描到目标（每个键带 4 字节长度前缀）
        while True:
            lb = os.pread(fd, 4, off)
            (ln,) = struct.unpack("<I", lb)
            if cur == local_id:
                return os.pread(fd, ln, off + 4).decode("utf-8")
            off += 4 + ln
            cur += 1

    def iter_ids(self):
        """按 local_id 升序流式产出全部身份字符串。

        全量顺序扫描走缓冲读，避免逐键 ``pread`` + 采样回退；供台账逐日
        提取结果时把局部编号翻译成原始 entity_id，内存占用有界。
        """
        with open(self.blob_path, "rb") as fh:
            while True:
                head = fh.read(4)
                if len(head) < 4:
                    return
                (ln,) = struct.unpack("<I", head)
                yield fh.read(ln).decode("utf-8")

    def iter_range(self, first_local: int, last_local: int):
        """按 local_id 升序流式产出 ``[first_local, last_local)`` 的身份。

        先二分到最近采样点，再用独立缓冲句柄顺序读取；不逐键 ``pread``，
        也不把整段身份装进内存。只读取调用者指定的局部编号区间。
        """
        if first_local >= last_local:
            return
        self.open()
        assert self._fh is not None
        if not self._index:
            raise InputDataError("身份 blob 缺少采样索引")
        lo, hi = 0, len(self._index) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if self._index[mid][0] <= first_local:
                lo = mid
            else:
                hi = mid - 1
        cur, off = self._index[lo]
        with open(self.blob_path, "rb") as fh:
            fh.seek(off)
            while cur < first_local:
                head = fh.read(4)
                if len(head) < 4:
                    raise InputDataError("身份 blob 截断")
                (ln,) = struct.unpack("<I", head)
                fh.seek(ln, 1)
                cur += 1
            while cur < last_local:
                head = fh.read(4)
                if len(head) < 4:
                    raise InputDataError("身份 blob 截断")
                (ln,) = struct.unpack("<I", head)
                data = fh.read(ln)
                if len(data) != ln:
                    raise InputDataError("身份 blob 截断")
                yield data.decode("utf-8")
                cur += 1


class IdBlobWriter:
    """构建期与 HashDirectory 并行写 blob（带 u32 长度前缀）。"""

    def __init__(self, blob_path: str, idx_path: str):
        self.blob = open(blob_path, "wb")
        self.idx = open(idx_path, "wb")
        self._cur = 0
        self._count = 0
        self.blob_path = blob_path
        self.idx_path = idx_path

    def add(self, key: str) -> Tuple[int, int]:
        if self._count % _IDX_SAMPLE == 0:
            self.idx.write(struct.pack("<QQ", self._count, self._cur))
        b = key.encode("utf-8")
        self.blob.write(struct.pack("<I", len(b)))
        self.blob.write(b)
        off = self._cur
        self._cur += 4 + len(b)
        self._count += 1
        return off, 4 + len(b)

    def close(self) -> None:
        self.blob.close()
        self.idx.close()


class ChildRangeTable:
    """pid -> [start_cid, start_cid+len) 的定长表，按 pid 升序。"""

    def __init__(self, path: str, n_parents: int):
        self.path = path
        self.n_parents = n_parents
        self._fh: Optional[BinaryIO] = None
        self._mm: Optional[mmap.mmap] = None

    def build_empty(self) -> None:
        with open(self.path, "wb") as fh:
            rec = _RANGE.pack(0, 0)
            fh.write(rec * self.n_parents)

    def open(self) -> None:
        if self._fh is None:
            self._fh = open(self.path, "rb")
            # 定长只读表，mmap 不占用 RSS（按需调页），大小 16*n_parents
            self._mm = mmap.mmap(
                self._fh.fileno(), 0, prot=mmap.PROT_READ
            )

    def close(self) -> None:
        if self._mm is not None:
            self._mm.close()
            self._mm = None
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def range_of(self, pid: int) -> Tuple[int, int]:
        self.open()
        assert self._mm is not None
        start, length = _RANGE.unpack_from(self._mm, pid * _RANGE.size)
        return start, start + length

    def iter_nonempty(self):
        """流式产出 (pid, start, end)，不在内存构造全表列表。"""
        self.open()
        assert self._mm is not None
        for pid in range(self.n_parents):
            start, length = _RANGE.unpack_from(self._mm, pid * _RANGE.size)
            if length:
                yield pid, start, start + length

    def iter_all(self):
        """流式产出全部 (pid, start, end)，含空组。"""
        self.open()
        assert self._mm is not None
        for pid in range(self.n_parents):
            start, length = _RANGE.unpack_from(self._mm, pid * _RANGE.size)
            yield pid, start, start + length

    def all_ranges(self) -> List[Tuple[int, int, int]]:
        """保留兼容接口；新代码优先用 iter_nonempty 避免 O(n) 列表。"""
        return list(self.iter_nonempty())


class IdentityStore:
    """一个索引的完整身份层（构建/打开两用）。"""

    def __init__(self, index_dir: str, load_factor: float = 0.7):
        self.dir = os.path.join(index_dir, "ident")
        os.makedirs(self.dir, exist_ok=True)
        self.load_factor = load_factor
        self.n_parents = 0
        self.n_children = 0
        self.parent_hash: Optional[HashDirectory] = None
        self.child_hash: Optional[HashDirectory] = None
        self.parent_blob = _IdBlob(
            os.path.join(self.dir, "parent.blob"),
            os.path.join(self.dir, "parent.idx"),
        )
        self.child_blob = _IdBlob(
            os.path.join(self.dir, "child.blob"),
            os.path.join(self.dir, "child.idx"),
        )
        self.ranges: Optional[ChildRangeTable] = None

    def buckets_for(self, n: int) -> int:
        # 至少 16 槽，保证短表探测链短
        return max(16, int(n / self.load_factor) + 1)

    # ---- 父表构建（流式） ----------------------------------------------

    def begin_parent_build(self, n_parents_est: int) -> None:
        """开始流式父表构建；重复身份由哈希目录自身拒绝，无需内存 set。"""
        path = os.path.join(self.dir, "parent.hash")
        self._parent_buckets = self.buckets_for(max(1, n_parents_est))
        self._parent_hd = HashDirectory(path, self._parent_buckets)
        self._parent_hash_fh = self._parent_hd.init_writer()
        self._parent_blob = IdBlobWriter(
            os.path.join(self.dir, "parent.blob"),
            os.path.join(self.dir, "parent.idx"),
        )
        self._parent_count = 0

    def add_parent(self, entity_id: str) -> int:
        off, blen = self._parent_blob.add(entity_id)
        pid = self._parent_count
        # 哈希目录遇重复键直接抛 InputDataError
        self._parent_hd.insert(self._parent_hash_fh, entity_id, pid, off, blen)
        self._parent_count += 1
        return pid

    def finish_parent_build(self) -> int:
        fh = self._parent_hash_fh
        fh.flush()
        os.fsync(fh.fileno())
        fh.close()
        self._parent_blob.close()
        self.n_parents = self._parent_count
        path = os.path.join(self.dir, "parent.hash")
        self.parent_hash = HashDirectory(path, self._parent_buckets)
        self.parent_hash.open()
        self.ranges = ChildRangeTable(
            os.path.join(self.dir, "child_range.bin"), self.n_parents
        )
        self.ranges.build_empty()
        return self.n_parents

    # ---- 子表构建（流式） ----------------------------------------------

    def begin_child_build(self, n_children_est: int) -> None:
        path = os.path.join(self.dir, "child.hash")
        self._child_buckets = self.buckets_for(max(1, n_children_est))
        self._child_hd = HashDirectory(path, self._child_buckets)
        self._child_hash_fh = self._child_hd.init_writer()
        self._child_blob = IdBlobWriter(
            os.path.join(self.dir, "child.blob"),
            os.path.join(self.dir, "child.idx"),
        )
        self._child_count = 0

    def add_child(self, child_id: str) -> int:
        off, blen = self._child_blob.add(child_id)
        cid = self._child_count
        self._child_hd.insert(self._child_hash_fh, child_id, cid, off, blen)
        self._child_count += 1
        return cid

    def finish_child_build(self) -> int:
        fh = self._child_hash_fh
        fh.flush()
        os.fsync(fh.fileno())
        fh.close()
        self._child_blob.close()
        self.n_children = self._child_count
        return self.n_children

    def build_parent(self, entity_ids: Sequence[str]) -> None:
        """保留的批量接口（小夹具/测试用），内部走流式构建。"""
        self.begin_parent_build(len(entity_ids))
        for eid in entity_ids:
            self.add_parent(e)
        self.finish_parent_build()

    def write_child_range(self, pid: int, start_cid: int, length: int) -> None:
        """导入子表分片时由 importer 调用（range 文件先建空再 pwrite）。"""
        assert self.ranges is not None
        fh = open(self.ranges.path, "r+b")
        try:
            fh.seek(pid * _RANGE.size)
            fh.write(_RANGE.pack(start_cid, length))
        finally:
            fh.close()

    # ---- 打开只读索引 --------------------------------------------------

    def open(self, n_parents: int, n_children: int, parent_buckets: int,
             child_buckets: int) -> None:
        self.n_parents = n_parents
        self.n_children = n_children
        self.parent_hash = HashDirectory(
            os.path.join(self.dir, "parent.hash"), parent_buckets
        )
        self.parent_hash.open()
        if n_children:
            self.child_hash = HashDirectory(
                os.path.join(self.dir, "child.hash"), child_buckets
            )
            self.child_hash.open()
        self.ranges = ChildRangeTable(
            os.path.join(self.dir, "child_range.bin"), n_parents
        )
        self.ranges.open()

    def close(self) -> None:
        if self.parent_hash is not None:
            self.parent_hash.close()
        if self.child_hash is not None:
            self.child_hash.close()
        if self.ranges is not None:
            self.ranges.close()
        self.parent_blob.close()
        self.child_blob.close()

    # ---- 可逆查询 ------------------------------------------------------

    def pid_of(self, entity_id: str) -> Optional[int]:
        assert self.parent_hash is not None
        hit = self.parent_hash.lookup(entity_id)
        return None if hit is None else hit[0]

    def entity_id_at(self, pid: int) -> str:
        return self.parent_blob.id_at(pid)

    def iter_parent_ids(self):
        """按 pid 升序流式产出父 entity_id（与 :meth:`entity_id_at` 同序）。"""
        return self.parent_blob.iter_ids()

    def iter_child_ids(self, first_cid: int, last_cid: int):
        """按 cid 升序流式产出 ``[first_cid, last_cid)`` 的子身份。

        只读取该子组所在区间；调用者据此在导出阶段按组取身份，而不是
        先恢复整张子表。
        """
        return self.child_blob.iter_range(first_cid, last_cid)

    def cid_of(self, child_id: str) -> Optional[int]:
        if self.child_hash is None:
            return None
        hit = self.child_hash.lookup(child_id)
        return None if hit is None else hit[0]

    def child_id_at(self, cid: int) -> str:
        return self.child_blob.id_at(cid)

    def child_range(self, pid: int) -> Tuple[int, int]:
        assert self.ranges is not None
        return self.ranges.range_of(pid)
