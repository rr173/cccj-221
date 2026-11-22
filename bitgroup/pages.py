"""落盘分页位图访问层（纯标准库）。

设计要点
========

* 一个 *domain*（父/子各一个）由若干固定 65536 位（8 KiB）的页组成，
  页编号在该域内即局部编号高位，局部编号 ⇄ 主/子身份的可逆映射由
  ``ident`` 模块负责，本层只认页与页内偏移。
* 每个位图是一个独立文件：``dir`` 段记录每页数据位置与形态，``data`` 段
  保存原始 8 KiB 页或 RLE 压缩页。读取走 ``pread``，只把被触碰的页取进
  LRU 页缓存；稀疏条件不会读取无关属性文件，更不会整表载入。
* 执行期中间集合是 *临时位图*：页同样按需物化，脏页被 LRU 驱逐时顺序追加
  到 tmp spool；释放即回收。缓存驻留与逻辑规模都计入 :class:`IOMonitor`，
  从而可以观测临时位图峰值，而不必把不同条件的 posting 同时常驻。

页在磁盘上的形态：

* ``FLAG_EMPTY`` 全零页，无数据载荷；
* ``FLAG_RAW``  恰好 8192 字节的原始 64 位字序列；
* ``FLAG_RLE``  游程编码（头一个小端 u16 游程数，随后每游程 u64 值 + u16 字数）。
"""

from __future__ import annotations

import os
import struct
from collections import OrderedDict
from typing import Dict, Iterable, List, Optional, Tuple

from .iomon import IOMonitor

PAGE_BITS = 1 << 16                      # 每页 65536 位
PAGE_BYTES = PAGE_BITS // 8             # 8192
PAGE_WORDS = PAGE_BYTES // 8            # 1024 个 uint64
DIR_ENTRY = struct.Struct("<QII")        # data_offset, data_len, flags
_FULL = (1 << 64) - 1

FLAG_EMPTY = 0
FLAG_RAW = 1
FLAG_RLE = 2
# 临时位图页落 spool 后的私有标志
FLAG_TMP_RAW = 3
FLAG_TMP_RLE = 4
_TMP_FLAG_MAP = {FLAG_TMP_RAW: FLAG_RAW, FLAG_TMP_RLE: FLAG_RLE}


def shards_for(n_ids: int) -> int:
    """n 个局部编号需要的页数。"""
    if n_ids <= 0:
        return 0
    return (n_ids + PAGE_BITS - 1) // PAGE_BITS


def _rle_encode(words: List[int]) -> bytes:
    out = bytearray()
    runs: List[Tuple[int, int]] = []
    i = 0
    while i < PAGE_WORDS:
        v = words[i]
        j = i + 1
        while j < PAGE_WORDS and words[j] == v:
            j += 1
        runs.append((v, j - i))
        i = j
    if len(runs) > 65535:  # pragma: no cover - 1024 字内游程数上界 1024
        raise ValueError("游程数超过 u16")
    out += struct.pack("<H", len(runs))
    for v, cnt in runs:
        out += struct.pack("<QH", v, cnt)
    return bytes(out)


def _rle_decode(buf: bytes) -> List[int]:
    (n_runs,) = struct.unpack_from("<H", buf, 0)
    pos = 2
    words: List[int] = []
    for _ in range(n_runs):
        (v, cnt) = struct.unpack_from("<QH", buf, pos)
        pos += 10
        words.extend([v] * cnt)
    if len(words) != PAGE_WORDS:
        raise ValueError("RLE 页损坏：字数不为 1024")
    return words


def _words_runs(words: List[int]) -> int:
    runs = 1
    prev = words[0]
    for w in words[1:]:
        if w != prev:
            runs += 1
            prev = w
    return runs


class _PageCache:
    """按 (file_id, page_key) 驻留解压后页的 LRU 缓存。

    预算按字节计。每个缓存槽统一 8 KiB；临时位图脏页驱逐时通过
    ``on_evict`` 回调落盘，驻留字节的增减只在本类内发生。
    """

    def __init__(self, monitor: IOMonitor, budget_bytes: int):
        self.monitor = monitor
        self.budget = max(budget_bytes, PAGE_BYTES)
        self._lru: "OrderedDict[Tuple[int, int], dict]" = OrderedDict()

    def get(self, key: Tuple[int, int]):
        entry = self._lru.get(key)
        if entry is not None:
            self._lru.move_to_end(key)
            self.monitor.cache_hits += 1
        else:
            self.monitor.cache_misses += 1
        return entry

    def put(self, key: Tuple[int, int], entry: dict) -> None:
        if key in self._lru:
            self._lru.move_to_end(key)
            return
        self._lru[key] = entry
        self.monitor.cache_resident_bytes += PAGE_BYTES
        if self.monitor.cache_resident_bytes > self.monitor.cache_resident_peak:
            self.monitor.cache_resident_peak = self.monitor.cache_resident_bytes
        if entry.get("temp"):
            self.monitor.temp_resident_bytes += PAGE_BYTES
            if self.monitor.temp_resident_bytes > self.monitor.temp_resident_peak:
                self.monitor.temp_resident_peak = self.monitor.temp_resident_bytes
        self._enforce()

    def discard(self, key: Tuple[int, int]) -> None:
        entry = self._lru.pop(key, None)
        if entry is None:
            return
        self.monitor.cache_resident_bytes -= PAGE_BYTES
        if entry.get("temp"):
            self.monitor.temp_resident_bytes -= PAGE_BYTES

    def _enforce(self) -> None:
        while self.monitor.cache_resident_bytes > self.budget and len(self._lru) > 1:
            old_key, old_entry = self._lru.popitem(last=False)
            self.monitor.cache_evictions += 1
            self.monitor.cache_resident_bytes -= PAGE_BYTES
            if old_entry.get("temp"):
                self.monitor.temp_resident_bytes -= PAGE_BYTES
            on_evict = old_entry.get("on_evict")
            if on_evict is not None:
                on_evict(old_key, old_entry)

    def __len__(self) -> int:
        return len(self._lru)


class BitmapFile:
    """一个持久只读位图文件（posting 或 null 位图）。"""

    def __init__(
        self,
        path: str,
        file_id: int,
        n_pages: int,
        cache: _PageCache,
        monitor: IOMonitor,
        count_files: bool = True,
    ):
        self.path = path
        self.file_id = file_id
        self.n_pages = n_pages
        self._cache = cache
        self._mon = monitor
        self._fh = open(path, "rb")
        self._dir: Optional[List[Tuple[int, int, int]]] = None
        if count_files:
            monitor.posting_files_opened += 1

    def _ensure_dir(self) -> None:
        if self._dir is None:
            file_size = os.fstat(self._fh.fileno()).st_size
            dir_start_off = file_size - 8
            (dir_start,) = struct.unpack(
                "<Q", os.pread(self._fh.fileno(), 8, dir_start_off)
            )
            raw = os.pread(
                self._fh.fileno(), DIR_ENTRY.size * self.n_pages, dir_start
            )
            if len(raw) != DIR_ENTRY.size * self.n_pages:
                raise ValueError(f"位图目录损坏: {self.path}")
            self._mon.page_reads += 1
            self._mon.bytes_read += len(raw) + 8
            self._dir = [
                DIR_ENTRY.unpack_from(raw, i * DIR_ENTRY.size)
                for i in range(self.n_pages)
            ]
    def read_page(self, page_no: int) -> Optional[List[int]]:
        """返回该页 1024 个 uint64；全零页返回 None。"""
        if page_no >= self.n_pages:
            return None
        self._ensure_dir()
        key = ("perm", self.file_id, page_no)
        entry = self._cache.get(key)
        if entry is not None:
            return entry["words"]
        offset, dlen, flags = self._dir[page_no]
        if flags == FLAG_EMPTY:
            return None
        raw = os.pread(self._fh.fileno(), dlen, offset)
        self._mon.page_reads += 1
        self._mon.bytes_read += len(raw)
        self._mon.posting_pages_touched += 1
        if flags == FLAG_RAW:
            if len(raw) != PAGE_BYTES:
                raise ValueError(f"RAW 页损坏: {self.path} p{page_no}")
            words = list(struct.unpack("<1024Q", raw))
        elif flags == FLAG_RLE:
            words = _rle_decode(raw)
        else:
            raise ValueError(f"未知页标志 {flags}: {self.path}")
        self._cache.put(key, {"words": words, "dirty": False, "temp": False})
        return words

    def close(self) -> None:
        try:
            self._fh.close()
        finally:
            for p in range(self.n_pages):
                self._cache.discard(("perm", self.file_id, p))


class BitmapWriter:
    """导入期顺序写出一个持久位图文件。"""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._fh = open(path, "w+b")
        self._dir: List[Tuple[int, int, int]] = []
        self._offset = 0

    def write_page(self, words: Optional[List[int]]) -> None:
        if words is None or not any(words):
            self._dir.append((0, 0, FLAG_EMPTY))
            return
        rle_len = 2 + _words_runs(words) * 10
        if rle_len < PAGE_BYTES:
            payload = _rle_encode(words)
            flags = FLAG_RLE
        else:
            payload = struct.pack("<1024Q", *words)
            flags = FLAG_RAW
        self._fh.write(payload)
        self._dir.append((self._offset, len(payload), flags))
        self._offset += len(payload)

    def finish(self, n_pages: int) -> None:
        if len(self._dir) != n_pages:
            raise AssertionError("写出页数与声明页数不一致")
        dir_start = self._offset
        for off, dlen, flags in self._dir:
            self._fh.write(DIR_ENTRY.pack(off, dlen, flags))
        self._fh.write(struct.pack("<Q", dir_start))  # 目录起点
        # 只 flush 到内核缓冲，不逐文件 fsync（构建末尾由 sync_all 统一刷盘，
        # 避免上千次 fsync 主导构建耗时）
        self._fh.flush()
        self._fh.close()

    def fsync(self) -> None:
        self._fh.flush()
        os.fsync(self._fh.fileno())


class TempSpool:
    """所有临时位图共享的顺序追加 spool 文件。"""

    def __init__(self, path: str, file_id: int):
        self.path = path
        self.file_id = file_id
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.fh = open(path, "w+b")
        self._append_offset = 0
        self._next_serial = 1
        self._live: Dict[int, "TempBitmap"] = {}

    def append(self, payload: bytes) -> int:
        off = self._append_offset
        self.fh.write(payload)
        # 读回走 os.pread（绕过 Python 缓冲），必须先把缓冲刷入内核
        self.fh.flush()
        self._append_offset += len(payload)
        return off

    def alloc_serial(self) -> int:
        s = self._next_serial
        self._next_serial += 1
        return s

    def register(self, tb: "TempBitmap") -> None:
        self._live[tb.serial] = tb

    def release_temp(self, tb: "TempBitmap") -> None:
        self._live.pop(tb.serial, None)

    def close(self) -> None:
        for tb in list(self._live.values()):
            tb.release()
        self.fh.close()
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass
        # 清理空的 tmp 目录（仅当确为空时）
        tmp_dir = os.path.dirname(self.path)
        try:
            os.rmdir(tmp_dir)
        except OSError:
            pass


class TempBitmap:
    """执行期临时位图，按需物化页，脏页可被 LRU 驱逐到 tmp spool。"""

    def __init__(
        self,
        tmp: TempSpool,
        serial: int,
        n_pages: int,
        cache: _PageCache,
        monitor: IOMonitor,
        label: str = "",
    ):
        self._tmp = tmp
        self.serial = serial
        self.n_pages = n_pages
        self._cache = cache
        self._mon = monitor
        self.label = label
        self.released = False
        # 每页磁盘位置：None=从未落盘；(offset,len,flags)
        self._spooled: List[Optional[Tuple[int, int, int]]] = [None] * n_pages
        monitor.temp_live_count += 1
        monitor.temp_live_logical += n_pages * PAGE_BYTES
        if monitor.temp_live_count > monitor.temp_live_count_peak:
            monitor.temp_live_count_peak = monitor.temp_live_count
        if monitor.temp_live_logical > monitor.temp_live_logical_peak:
            monitor.temp_live_logical_peak = monitor.temp_live_logical

    # ---- 页获取 --------------------------------------------------------

    def _key(self, page_no: int) -> Tuple[str, int, int]:
        # 序列号全局唯一，TempBitmap 释放后也不会与新位图撞键
        return ("temp", self.serial, page_no)

    def _page(self, page_no: int, *, create: bool) -> Optional[List[int]]:
        key = self._key(page_no)
        entry = self._cache.get(key)
        if entry is not None:
            return entry["words"]
        loc = self._spooled[page_no]
        if loc is None:
            if not create:
                return None
            words = [0] * PAGE_WORDS
        else:
            offset, dlen, flags = loc
            raw = os.pread(self._tmp.fh.fileno(), dlen, offset)
            self._mon.page_reads += 1
            self._mon.bytes_read += len(raw)
            if flags == FLAG_TMP_RAW:
                words = list(struct.unpack("<1024Q", raw))
            elif flags == FLAG_TMP_RLE:
                words = _rle_decode(raw)
            else:
                raise ValueError("临时页标志损坏")
        entry = {
            "words": words,
            "dirty": create,
            "temp": True,
            "on_evict": self._evict_cb,
        }
        self._cache.put(key, entry)
        return words

    def _evict_cb(self, key: Tuple[str, int, int], entry: dict) -> None:
        """缓存层已扣减驻留字节；这里只负责把脏页落盘。"""
        if not entry.get("dirty"):
            return
        page_no = key[2]
        words = entry["words"]
        rle_len = 2 + _words_runs(words) * 10
        if rle_len < PAGE_BYTES:
            payload = _rle_encode(words)
            flags = FLAG_TMP_RLE
        else:
            payload = struct.pack("<1024Q", *words)
            flags = FLAG_TMP_RAW
        offset = self._tmp.append(payload)
        self._spooled[page_no] = (offset, len(payload), flags)
        entry["dirty"] = False
        entry.pop("on_evict", None)
        self._mon.page_writes += 1
        self._mon.bytes_written += len(payload)

    def get_page(self, page_no: int) -> Optional[List[int]]:
        if page_no >= self.n_pages:
            return None
        return self._page(page_no, create=False)

    def mutable_page(self, page_no: int) -> List[int]:
        return self._page(page_no, create=True)  # type: ignore[return-value]

    def set_bit(self, local_id: int) -> None:
        page_no, bit = divmod(local_id, PAGE_BITS)
        words = self.mutable_page(page_no)
        words[bit >> 6] |= 1 << (bit & 63)

    def get_bit(self, local_id: int) -> bool:
        page_no, bit = divmod(local_id, PAGE_BITS)
        words = self.get_page(page_no)
        if words is None:
            return False
        return bool(words[bit >> 6] & (1 << (bit & 63)))

    def or_page_bits(self, page_no: int, words: List[int]) -> None:
        cur = self.mutable_page(page_no)
        for i, w in enumerate(words):
            cur[i] |= w

    def fill_page(self, page_no: int, n_bits_in_page: int) -> None:
        cur = self.mutable_page(page_no)
        full, rem = divmod(n_bits_in_page, 64)
        for i in range(full):
            cur[i] = _FULL
        if rem:
            cur[full] |= (1 << rem) - 1

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        for p in range(self.n_pages):
            self._cache.discard(self._key(p))
        self._mon.temp_live_count -= 1
        self._mon.temp_live_logical -= self.n_pages * PAGE_BYTES
        self._tmp.release_temp(self)

    def persist(self, dest_path: str) -> None:
        """把临时位图固化成普通只读位图文件（查询结果集合用）。"""
        w = BitmapWriter(dest_path)
        for p in range(self.n_pages):
            w.write_page(self.get_page(p))
        w.finish(self.n_pages)


class BitmapStore:
    """一个索引目录内的全部位图资源：父/子两个编号域 + 临时 spool。"""

    def __init__(self, index_dir: str, monitor: IOMonitor, cache_budget_bytes: int):
        self.index_dir = index_dir
        self.monitor = monitor
        self.cache = _PageCache(monitor, cache_budget_bytes)
        self._next_file_id = 1
        self._open_files: Dict[int, BitmapFile] = {}
        self._spool: Optional[TempSpool] = None
        self.parent_n_pages = 0
        self.child_n_pages = 0

    # ---- 导入期 --------------------------------------------------------

    def writer(self, relative_path: str) -> BitmapWriter:
        return BitmapWriter(os.path.join(self.index_dir, relative_path))

    # ---- 查询期 --------------------------------------------------------

    def open_bitmap(
        self, relative_path: str, n_pages: int, *, count_files: bool = True
    ) -> BitmapFile:
        path = os.path.join(self.index_dir, relative_path)
        fid = self._next_file_id
        self._next_file_id += 1
        bf = BitmapFile(path, fid, n_pages, self.cache, self.monitor, count_files)
        self._open_files[fid] = bf
        return bf

    def _ensure_spool(self) -> TempSpool:
        if self._spool is None:
            fid = self._next_file_id
            self._next_file_id += 1
            self._spool = TempSpool(
                os.path.join(self.index_dir, "tmp", "spool.bit"), fid
            )
        return self._spool

    def new_temp(self, n_pages: int, label: str = "") -> TempBitmap:
        spool = self._ensure_spool()
        serial = spool.alloc_serial()
        tb = TempBitmap(spool, serial, n_pages, self.cache, self.monitor, label)
        spool.register(tb)
        return tb

    def close(self) -> None:
        for bf in list(self._open_files.values()):
            bf.close()
        self._open_files.clear()
        if self._spool is not None:
            self._spool.close()
            self._spool = None

    # ---- 整页级集合运算 ------------------------------------------------

    @staticmethod
    def page_and(a: Optional[List[int]], b: Optional[List[int]]):
        if a is None or b is None:
            return None
        return [x & y for x, y in zip(a, b)]

    @staticmethod
    def page_or(a: Optional[List[int]], b: Optional[List[int]]):
        if a is None:
            return None if b is None else list(b)
        if b is None:
            return list(a)
        return [x | y for x, y in zip(a, b)]

    @staticmethod
    def page_andnot(a: Optional[List[int]], b: Optional[List[int]]):
        """a AND NOT b（b 为 None 时退化为 a）。"""
        if a is None:
            return None
        if b is None:
            return list(a)
        return [x & ~y for x, y in zip(a, b)]

    @staticmethod
    def page_complement(a: Optional[List[int]], n_bits_in_page: int) -> List[int]:
        """域内补集：页内有效位取反，无效位保持 0。"""
        out = [0] * PAGE_WORDS
        full, rem = divmod(n_bits_in_page, 64)
        if a is None:
            for i in range(full):
                out[i] = _FULL
            if rem:
                out[full] = (1 << rem) - 1
            return out
        for i in range(full):
            out[i] = ~a[i] & _FULL
        if rem:
            out[full] = (~a[full]) & ((1 << rem) - 1)
        return out

    @staticmethod
    def page_any(words: Optional[List[int]]) -> bool:
        return words is not None and any(words)

    @staticmethod
    def page_popcount(words: Optional[List[int]]) -> int:
        if words is None:
            return 0
        return sum(w.bit_count() for w in words)

    @staticmethod
    def iter_set_bits(tb: TempBitmap, total_ids: int) -> Iterable[int]:
        """枚举临时位图中置位的局部编号。"""
        for p in range(tb.n_pages):
            words = tb.get_page(p)
            if words is None:
                continue
            base = p * PAGE_BITS
            for wi, w in enumerate(words):
                while w:
                    low = w & -w
                    bit = low.bit_length() - 1
                    local = base + wi * 64 + bit
                    if local >= total_ids:
                        return
                    yield local
                    w ^= low
