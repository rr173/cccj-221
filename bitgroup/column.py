"""列存与倒排 posting。

两套面向不同访问模式的物理结构，互不替代：

* **列存**（``col/<domain>/<attr>.*``）按局部编号顺序保存每行的原始值与
  null 标记，供 explain-entity 的点查与见证读取；
* **posting**（``post/<domain>/<attr>/...``）是“取值 -> 位图”的倒排结构，
  每个取值一个独立位图文件，引擎只打开条件真正触碰的取值文件。

int64 的取值索引是有序值表（外部基数分区排序，构建不依赖全量驻留）；
text 的取值索引是磁盘哈希目录；bool 固定 true/false 两个位图；
null 位图每个属性单列。
"""

from __future__ import annotations

import os
import struct
from typing import BinaryIO, Dict, List, Optional, Tuple

from .errors import InputDataError
from .ident import TextHashDirectory
from .pages import BitmapWriter, shards_for

_U32 = struct.Struct("<I")
_I64_U32 = struct.Struct("<qI")
_QII = struct.Struct("<QII")
_IDX_SAMPLE = 1024
_NULL_DICT_ID = 0xFFFFFFFF
_TMP_PAGE_BITS = 1 << 16


# --------------------------------------------------------------------------
# 构建期：列存
# --------------------------------------------------------------------------


class IntColumnWriter:
    """int64 列：定宽 i64 值文件 + (value,local_id) 记录流 + null id 流。"""

    def __init__(self, base_path: str):
        self.val_fh = open(base_path + ".i64", "wb")
        self.rec_fh = open(base_path + ".rec", "wb")
        self.null_fh = open(base_path + ".null", "wb")

    def add(self, value, local_id: int) -> None:
        if value is None:
            self.val_fh.write(struct.pack("<q", 0))
            self.null_fh.write(_U32.pack(local_id))
        else:
            self.val_fh.write(struct.pack("<q", value))
            self.rec_fh.write(_I64_U32.pack(value, local_id))

    def close(self) -> None:
        for fh in (self.val_fh, self.rec_fh, self.null_fh):
            fh.close()


class BoolColumnWriter:
    """bool 列：每字节 0/1（null 行写 0）+ null id 流。"""

    def __init__(self, base_path: str):
        self.val_fh = open(base_path + ".bool", "wb")
        self.null_fh = open(base_path + ".null", "wb")

    def add(self, value, local_id: int) -> None:
        if value is None:
            self.val_fh.write(b"\x00")
            self.null_fh.write(_U32.pack(local_id))
        else:
            self.val_fh.write(b"\x01" if value else b"\x00")

    def close(self) -> None:
        self.val_fh.close()
        self.null_fh.close()


class TextColumnWriter:
    """text 列：定宽 u32 dict_id 文件（null=0xFFFFFFFF）+ 字符串字典。

    distinct 字符串保存在内存 dict 中（text 属性本就是低基数分类语义）；
    字典同时落盘为 blob（长度前缀）+ 定长槽哈希目录，供点查与等值 posting。
    """

    def __init__(self, base_path: str):
        self.base_path = base_path
        self.dict_dir = base_path + "_d"
        os.makedirs(self.dict_dir, exist_ok=True)
        self.val_fh = open(base_path + ".dict", "wb")
        self.null_fh = open(base_path + ".null", "wb")
        self.blob_fh = open(os.path.join(self.dict_dir, "strings.blob"), "wb")
        self.idx_fh = open(os.path.join(self.dict_dir, "strings.idx"), "wb")
        self._map: Dict[str, int] = {}
        self._cur_blob = 0

    def add(self, value, local_id: int) -> None:
        if value is None:
            self.val_fh.write(_U32.pack(_NULL_DICT_ID))
            self.null_fh.write(_U32.pack(local_id))
            return
        did = self._map.get(value)
        if did is None:
            did = len(self._map)
            self._map[value] = did
            b = value.encode("utf-8")
            if did % _IDX_SAMPLE == 0:
                self.idx_fh.write(struct.pack("<QQ", did, self._cur_blob))
            self.blob_fh.write(_U32.pack(len(b)))
            self.blob_fh.write(b)
            self._cur_blob += 4 + len(b)
        self.val_fh.write(_U32.pack(did))

    def close(self) -> None:
        self.val_fh.close()
        self.null_fh.close()
        self.blob_fh.close()
        self.idx_fh.close()
        n = len(self._map)
        buckets = max(16, int(n / 0.7) + 1)
        hd = TextHashDirectory(os.path.join(self.dict_dir, "strings.hash"), buckets)
        fh = hd.init_writer()
        try:
            for s, did in self._map.items():
                hd.insert(fh, s, did, did)  # dict_id 与 posting file_id 相同
            fh.flush()
            os.fsync(fh.fileno())
        finally:
            fh.close()
        # 保存 dict 规模供 posting 构建使用
        with open(os.path.join(self.dict_dir, "meta.bin"), "wb") as mf:
            mf.write(struct.pack("<Q", n))


class NullColumnWriter:
    def __init__(self, base_path: str):
        self.null_fh = open(base_path + ".null", "wb")

    def add(self, value, local_id: int) -> None:
        if value is not None:
            raise InputDataError("null 类型列出现非 null 值")
        self.null_fh.write(_U32.pack(local_id))

    def close(self) -> None:
        self.null_fh.close()


# --------------------------------------------------------------------------
# 查询期：列存点查
# --------------------------------------------------------------------------


class ColumnReader:
    """按局部编号读取单行值；null 返回 None。null 列恒返回 None。"""

    def __init__(self, base_path: str, col_type: str):
        import mmap
        self.col_type = col_type
        self._base = base_path
        # null id 升序写入；用 mmap + 二分判定，不把全量 id 读进 Python 堆
        self._null_mm: Optional[mmap.mmap] = None
        self._null_fh: Optional[BinaryIO] = None
        self._null_n = 0
        null_path = base_path + ".null"
        if os.path.exists(null_path) and os.path.getsize(null_path) > 0:
            self._null_fh = open(null_path, "rb")
            self._null_mm = mmap.mmap(
                self._null_fh.fileno(), 0, prot=mmap.PROT_READ
            )
            self._null_n = os.path.getsize(null_path) // 4
        self._val_fh: Optional[BinaryIO] = None
        self._dict_blob_fh: Optional[BinaryIO] = None
        self._dict_samples: Optional[List[Tuple[int, int]]] = None
        if col_type == "int64":
            self._val_fh = open(base_path + ".i64", "rb")
        elif col_type == "bool":
            self._val_fh = open(base_path + ".bool", "rb")
        elif col_type == "text":
            self._val_fh = open(base_path + ".dict", "rb")

    def _is_null(self, local_id: int) -> bool:
        if self._null_mm is None:
            return False
        # 升序 u32 数组上二分
        lo, hi = 0, self._null_n
        mm = self._null_mm
        while lo < hi:
            mid = (lo + hi) // 2
            (v,) = struct.unpack_from("<I", mm, mid * 4)
            if v < local_id:
                lo = mid + 1
            else:
                hi = mid
        if lo < self._null_n:
            (v,) = struct.unpack_from("<I", mm, lo * 4)
            return v == local_id
        return False

    def read(self, local_id: int):
        if self._is_null(local_id):
            return None
        if self.col_type == "null":
            return None
        assert self._val_fh is not None
        fd = self._val_fh.fileno()
        if self.col_type == "int64":
            (v,) = struct.unpack("<q", os.pread(fd, 8, local_id * 8))
            return v
        if self.col_type == "bool":
            return os.pread(fd, 1, local_id) == b"\x01"
        if self.col_type == "text":
            (did,) = struct.unpack("<I", os.pread(fd, 4, local_id * 4))
            if did == _NULL_DICT_ID:
                return None
            return self._read_dict(did)
        raise AssertionError(self.col_type)

    def _read_dict(self, dict_id: int) -> str:
        if self._dict_blob_fh is None:
            d = self._base + "_d"
            self._dict_blob_fh = open(os.path.join(d, "strings.blob"), "rb")
            with open(os.path.join(d, "strings.idx"), "rb") as idx:
                idata = idx.read()
            self._dict_samples = [
                struct.unpack_from("<QQ", idata, i)
                for i in range(0, len(idata), 16)
            ]
        assert self._dict_samples is not None
        lo, hi = 0, len(self._dict_samples) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if self._dict_samples[mid][0] <= dict_id:
                lo = mid
            else:
                hi = mid - 1
        cur, off = self._dict_samples[lo]
        fd = self._dict_blob_fh.fileno()
        while True:
            (ln,) = struct.unpack("<I", os.pread(fd, 4, off))
            if cur == dict_id:
                return os.pread(fd, ln, off + 4).decode("utf-8")
            off += 4 + ln
            cur += 1

    def close(self) -> None:
        if self._val_fh is not None:
            self._val_fh.close()
        if self._dict_blob_fh is not None:
            self._dict_blob_fh.close()
        if self._null_mm is not None:
            self._null_mm.close()
        if self._null_fh is not None:
            self._null_fh.close()


# --------------------------------------------------------------------------
# 外部基数分区排序（i64 定长记录，带 u32 local_id）
# --------------------------------------------------------------------------


def _counting_sort_8(vals, lids, order, shift):
    """对 order 索引按 (val+2^63) 的第 shift..shift+7 位做稳定计数排序。

    256 桶，计数/游标数组固定 256 元素，内存占用小。
    """
    n = len(order)
    counts = [0] * 256
    keys = [0] * n
    for idx, src_i in enumerate(order):
        u = (vals[src_i] + (1 << 63)) & 0xFFFFFFFFFFFFFFFF
        key = (u >> shift) & 0xFF
        keys[idx] = key
        counts[key] += 1
    starts = [0] * 256
    acc = 0
    for b in range(256):
        starts[b] = acc
        acc += counts[b]
    new_order = [0] * n
    cursor = starts[:]
    for idx in range(n):
        b = keys[idx]
        new_order[cursor[b]] = order[idx]
        cursor[b] += 1
    return new_order


def radix_sort_i64_records(in_path: str, work_dir: str,
                           max_records_per_chunk: int = 100000) -> str:
    """把 (i64 value, u32 local_id) 记录按 value 升序外部排序。

    有符号数先翻转符号位当作无符号序。采用递归 MSD 基数分区：每次按当前
    8 位把一个文件拆成 256 个桶文件，超过块上限的桶继续递归细分；小到可
    整块入内存的桶读入做剩余低位的 LSD 计数排序。这样即使取值域极窄、
    高 8 位严重倾斜，也会在更低位被逐步拆开，任何时刻驻留记录数有界，
    不会因单桶接近全量而撑爆内存。桶文件数量随递归有界（叶子 256/层）。
    """
    os.makedirs(work_dir, exist_ok=True)
    rec = _I64_U32
    dst = os.path.join(work_dir, "sorted_final.bin")
    if os.path.exists(dst):
        os.remove(dst)
    out = open(dst, "wb")
    seq = [0]

    def new_name() -> str:
        seq[0] += 1
        return os.path.join(work_dir, f"b{seq[0]}.bin")

    def emit_sorted(records: List[Tuple[int, int]]) -> None:
        # records: list of (val, lid)，整块做完整 64 位 LSD 8 位计数排序
        # （8 轮，含最高位 shift=56），保证叶子块自身全局有序。
        n = len(records)
        vals = [r[0] for r in records]
        order = list(range(n))
        for shift in (0, 8, 16, 24, 32, 40, 48, 56):
            counts = [0] * 256
            keys = [0] * n
            for idx, src_i in enumerate(order):
                u = (vals[src_i] + (1 << 63)) & 0xFFFFFFFFFFFFFFFF
                key = (u >> shift) & 0xFF
                keys[idx] = key
                counts[key] += 1
            starts = [0] * 256
            acc = 0
            for b in range(256):
                starts[b] = acc
                acc += counts[b]
            new_order = [0] * n
            cursor = starts[:]
            for idx in range(n):
                b = keys[idx]
                new_order[cursor[b]] = order[idx]
                cursor[b] += 1
            order = new_order
        buf = bytearray(n * rec.size)
        for i, src_i in enumerate(order):
            rec.pack_into(buf, i * rec.size, vals[src_i], records[src_i][1])
        out.write(buf)

    def partition_file(path: str, shift: int, count: int) -> None:
        if count == 0:
            os.remove(path)
            return
        if count <= max_records_per_chunk or shift < 0:
            with open(path, "rb") as fh:
                data = fh.read()
            records = [
                rec.unpack_from(data, i * rec.size)
                for i in range(count)
            ]
            emit_sorted(records)
            os.remove(path)
            return
        # 按当前 8 位拆 256 桶
        bucket_paths = [new_name() for _ in range(256)]
        bucket_fhs = [open(p, "wb", buffering=1 << 16) for p in bucket_paths]
        bucket_counts = [0] * 256
        try:
            with open(path, "rb") as fh:
                while True:
                    buf = fh.read(rec.size * 65536)
                    if not buf:
                        break
                    for off in range(0, len(buf), rec.size):
                        (val, lid) = rec.unpack_from(buf, off)
                        u = (val + (1 << 63)) & 0xFFFFFFFFFFFFFFFF
                        b = (u >> shift) & 0xFF
                        bucket_fhs[b].write(rec.pack(val, lid))
                        bucket_counts[b] += 1
        finally:
            for fh in bucket_fhs:
                fh.close()
        os.remove(path)
        for b in range(256):
            if bucket_counts[b]:
                partition_file(bucket_paths[b], shift - 8, bucket_counts[b])
            else:
                os.remove(bucket_paths[b])

    # 入口计数
    total = os.path.getsize(in_path) // rec.size
    # 复制输入为首个待分区文件（输入 .rec 需保留给后续 null 构建，不直接删）
    first = new_name()
    with open(in_path, "rb") as fin, open(first, "wb") as fout:
        while True:
            chunk = fin.read(1 << 20)
            if not chunk:
                break
            fout.write(chunk)
    try:
        partition_file(first, 56, total)
    finally:
        out.close()
    return dst


# --------------------------------------------------------------------------
# posting 构建
# --------------------------------------------------------------------------


def _write_null_bitmap(null_path: str, out_path: str, n_pages: int) -> None:
    w = BitmapWriter(out_path)
    page_words: Optional[List[int]] = None
    cur_page = -1
    if os.path.exists(null_path):
        with open(null_path, "rb") as fh:
            data = fh.read()
        for i in range(0, len(data), 4):
            (lid,) = struct.unpack_from("<I", data, i)
            page_no, bit = divmod(lid, _TMP_PAGE_BITS)
            while cur_page < page_no:
                if page_words is not None:
                    w.write_page(page_words)
                cur_page += 1
                page_words = None if cur_page < page_no else [0] * 1024
            assert page_words is not None
            page_words[bit >> 6] |= 1 << (bit & 63)
    if page_words is not None:
        w.write_page(page_words)
    for _ in range(cur_page + 1, n_pages):
        w.write_page(None)
    w.finish(n_pages)


class PostingBuilder:
    """把一列物化成“取值 -> 位图文件 + 取值索引”。"""

    def __init__(self, post_dir: str, domain: str, n_rows: int):
        self.domain = domain
        self.n_rows = n_rows
        self.n_pages = shards_for(n_rows)
        self.dir = os.path.join(post_dir, domain)
        os.makedirs(self.dir, exist_ok=True)

    def _attr_dir(self, attr: str) -> str:
        d = os.path.join(self.dir, attr)
        os.makedirs(d, exist_ok=True)
        return d

    def build_empty_attr(self, attr: str, col_type: str) -> None:
        """空域（0 行）：空 null 位图 + 空取值索引（text 还需空 hash 目录）。"""
        d = self._attr_dir(attr)
        _write_null_bitmap(
            os.path.join(d, "null.bit.null"),  # 不存在的 null id 流
            os.path.join(d, "null.bit"),
            self.n_pages,
        )
        open(os.path.join(d, "values.idx"), "wb").close()
        if col_type == "text":
            buckets = 16
            hd = TextHashDirectory(os.path.join(d, "strings.hash"), buckets)
            fh = hd.init_writer()
            fh.close()

    def build_int(self, attr: str, base_path: str, work_dir: str) -> None:
        d = self._attr_dir(attr)
        sorted_path = radix_sort_i64_records(base_path + ".rec", work_dir)
        file_id = 0
        idx_fh = open(os.path.join(d, "values.idx"), "wb")
        cur_value = None
        writer: Optional[BitmapWriter] = None
        page_words: Optional[List[int]] = None
        cur_page = -1
        try:
            with open(sorted_path, "rb") as fin:
                total = os.path.getsize(sorted_path) // _I64_U32.size
                for _ in range(total):
                    val, lid = _I64_U32.unpack(fin.read(_I64_U32.size))
                    if val != cur_value:
                        if writer is not None:
                            if page_words is not None:
                                writer.write_page(page_words)
                                page_words = None
                            for _ in range(cur_page + 1, self.n_pages):
                                writer.write_page(None)
                            writer.finish(self.n_pages)
                        writer = BitmapWriter(os.path.join(d, f"v{file_id}.bit"))
                        idx_fh.write(struct.pack("<qI", val, file_id))
                        file_id += 1
                        cur_value = val
                        cur_page = -1
                    page_no, bit = divmod(lid, _TMP_PAGE_BITS)
                    while cur_page < page_no:
                        if page_words is not None:
                            writer.write_page(page_words)
                            page_words = None
                        cur_page += 1
                        if cur_page < page_no:
                            writer.write_page(None)
                        else:
                            page_words = [0] * 1024
                    assert page_words is not None
                    page_words[bit >> 6] |= 1 << (bit & 63)
            if writer is not None:
                if page_words is not None:
                    writer.write_page(page_words)
                for _ in range(cur_page + 1, self.n_pages):
                    writer.write_page(None)
                writer.finish(self.n_pages)
        finally:
            idx_fh.close()
            os.remove(sorted_path)
        _write_null_bitmap(
            base_path + ".null", os.path.join(d, "null.bit"), self.n_pages
        )

    def build_text(self, attr: str, base_path: str) -> None:
        d = self._attr_dir(attr)
        dict_dir = base_path + "_d"
        with open(os.path.join(dict_dir, "meta.bin"), "rb") as mf:
            (n_dict,) = struct.unpack("<Q", mf.read(8))
        # 每个 dict_id 一个 writer，只保留各自当前页；dict 列按块流式读取。
        writers = [
            BitmapWriter(os.path.join(d, f"v{did}.bit"))
            for did in range(n_dict)
        ]
        cur_page = [-1] * n_dict
        pages: List[Optional[List[int]]] = [None] * n_dict
        chunk = 1 << 18  # 每次处理 262144 个 u32 = 1 MiB
        lid = 0
        with open(base_path + ".dict", "rb") as fh:
            while True:
                buf = fh.read(chunk * 4)
                if not buf:
                    break
                for off in range(0, len(buf), 4):
                    (did,) = struct.unpack_from("<I", buf, off)
                    if did != _NULL_DICT_ID:
                        page_no, bit = divmod(lid, _TMP_PAGE_BITS)
                        while cur_page[did] < page_no:
                            if pages[did] is not None:
                                writers[did].write_page(pages[did])
                                pages[did] = None
                            cur_page[did] += 1
                            if cur_page[did] < page_no:
                                writers[did].write_page(None)
                            else:
                                pages[did] = [0] * 1024
                        assert pages[did] is not None
                        pages[did][bit >> 6] |= 1 << (bit & 63)
                    lid += 1
        n_rows = lid
        for did, w in enumerate(writers):
            if pages[did] is not None:
                w.write_page(pages[did])
            for _ in range(cur_page[did] + 1, self.n_pages):
                w.write_page(None)
            w.finish(self.n_pages)
        # 复用字典哈希目录为 posting 取值索引（payload 第二字即 file_id=dict_id）
        src_hash = os.path.join(dict_dir, "strings.hash")
        with open(src_hash, "rb") as fin, \
                open(os.path.join(d, "strings.hash"), "wb") as fout:
            while True:
                b = fin.read(1 << 20)
                if not b:
                    break
                fout.write(b)
        _write_null_bitmap(
            base_path + ".null", os.path.join(d, "null.bit"), self.n_pages
        )

    def build_bool(self, attr: str, base_path: str) -> None:
        d = self._attr_dir(attr)
        null_path = base_path + ".null"
        null_total = os.path.getsize(null_path) // 4 if os.path.exists(null_path) else 0
        null_idx = 0  # 下一个待消费的 null 序号（null id 按升序写入）
        # 一次性把 null id 读入数组的做法有 O(null) 内存；这里改为按需读，
        # 用有界页集合 + 单调游标，句柄始终顺序前进。
        w_true = BitmapWriter(os.path.join(d, "v1.bit"))
        w_false = BitmapWriter(os.path.join(d, "v0.bit"))
        with open(base_path + ".bool", "rb") as vfh, \
                open(null_path, "rb") as nfh:
            for p in range(self.n_pages):
                start = p * _TMP_PAGE_BITS
                end = min(start + _TMP_PAGE_BITS, self.n_rows)
                page_nulls: set = set()
                # 顺序消费 null id，直到读到 >= end（留给后续页）
                while null_idx < null_total:
                    raw = nfh.read(4)
                    (nid,) = struct.unpack("<I", raw)
                    if nid >= end:
                        # 该 id 属于后续页：记录“多读了一个”，下一页先消化它
                        nfh.seek(-4, 1)
                        break
                    null_idx += 1
                    if nid >= start:
                        page_nulls.add(nid)
                vals = vfh.read(end - start)
                wt = [0] * 1024
                wf = [0] * 1024
                any_t = any_f = False
                for lid in range(start, end):
                    if lid in page_nulls:
                        continue
                    bit = lid - start
                    if vals[lid - start] == 1:
                        wt[bit >> 6] |= 1 << (bit & 63)
                        any_t = True
                    else:
                        wf[bit >> 6] |= 1 << (bit & 63)
                        any_f = True
                w_true.write_page(wt if any_t else None)
                w_false.write_page(wf if any_f else None)
        w_true.finish(self.n_pages)
        w_false.finish(self.n_pages)
        open(os.path.join(d, "values.idx"), "wb").close()
        _write_null_bitmap(
            null_path, os.path.join(d, "null.bit"), self.n_pages
        )

    def build_null_only(self, attr: str, base_path: str) -> None:
        d = self._attr_dir(attr)
        open(os.path.join(d, "values.idx"), "wb").close()
        _write_null_bitmap(
            base_path + ".null", os.path.join(d, "null.bit"), self.n_pages
        )
