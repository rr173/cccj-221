"""posting 读取层：把属性比较翻译成按需打开的位图文件。

引擎对每个 ``cmp`` 节点只打开条件真正涉及的取值位图；int 范围条件通过
有序值索引二分得到候选文件 id 再逐个打开，等值 text 只做一次哈希探测。
null 位图也是独立文件，不与取值 posting 混驻。
"""

from __future__ import annotations

import os
import struct
from typing import Any, Dict, List, Optional, Tuple

from .errors import QueryError
from .ident import TextHashDirectory, fnv1a64
from .pages import BitmapFile, BitmapStore

_VALUE_REC = struct.Struct("<qI")
_QII = struct.Struct("<QII")


class AttrPosting:
    """单个属性的取值索引 + 惰性位图打开。"""

    def __init__(
        self,
        store: BitmapStore,
        attr_dir: str,
        col_type: str,
        n_pages: int,
        col_reader=None,
    ):
        self._store = store
        self.dir = attr_dir
        self.col_type = col_type
        self.n_pages = n_pages
        self._col_reader = col_reader  # text 反查字符串用
        self._int_values: Optional[List[Tuple[int, int]]] = None
        self._text_hash: Optional[TextHashDirectory] = None
        self._text_buckets = 0
        self._open: Dict[int, BitmapFile] = {}
        self._null_file: Optional[BitmapFile] = None

    # ---- 取值索引 ------------------------------------------------------

    def _load_int_index(self) -> List[Tuple[int, int]]:
        if self._int_values is None:
            path = os.path.join(self.dir, "values.idx")
            with open(path, "rb") as fh:
                data = fh.read()
            self._int_values = [
                _VALUE_REC.unpack_from(data, i)
                for i in range(0, len(data), _VALUE_REC.size)
            ]
        return self._int_values

    def _text_dir(self) -> TextHashDirectory:
        if self._text_hash is None:
            path = os.path.join(self.dir, "strings.hash")
            self._text_buckets = os.path.getsize(path) // _QII.size
            self._text_hash = TextHashDirectory(path, self._text_buckets)
            self._text_hash.open()
        return self._text_hash

    # ---- 位图打开 ------------------------------------------------------

    def _open_value(self, file_id: int) -> BitmapFile:
        bf = self._open.get(file_id)
        if bf is None:
            bf = self._store.open_bitmap(
                os.path.join(self.dir, f"v{file_id}.bit"), self.n_pages
            )
            self._open[file_id] = bf
        return bf

    def open_null(self) -> BitmapFile:
        if self._null_file is None:
            self._null_file = self._store.open_bitmap(
                os.path.join(self.dir, "null.bit"), self.n_pages,
                count_files=False,
            )
        return self._null_file

    # ---- 比较 -> 位图文件列表 -----------------------------------------

    def compare_files(self, op_kind: str, value) -> List[BitmapFile]:
        """返回使 左值 op_kind value 为 true 的非 null 行位图文件。"""
        if value is None:
            return []
        if self.col_type == "bool":
            if op_kind not in ("eq", "ne"):
                raise QueryError("bool 属性只支持 eq/ne 比较")
            target = 1 if value else 0
            if op_kind == "ne":
                target ^= 1
            return [self._open_value(target)]
        if self.col_type == "text":
            if op_kind not in ("eq", "ne"):
                raise QueryError("text 属性只支持 eq/ne 比较")
            rec = self._text_dir().lookup_hash(fnv1a64(value.encode("utf-8")))
            if rec is None:
                if op_kind == "eq":
                    return []
                return [bf for _v, bf in self.value_files_with_values()]
            _dict_id, file_id = rec[1], rec[2]
            bf = self._open_value(file_id)
            if op_kind == "eq":
                return [bf]
            return [
                x for v, x in self.value_files_with_values() if v != value
            ]
        # int64
        values = self._load_int_index()
        ids = [v for v, _fid in values]
        if op_kind == "eq":
            idx = self._bisect(ids, value)
            if idx < len(ids) and ids[idx] == value:
                return [self._open_value(values[idx][1])]
            return []
        if op_kind == "ne":
            idx = self._bisect(ids, value)
            skip = values[idx][1] if idx < len(ids) and ids[idx] == value else None
            return [
                self._open_value(fid)
                for v, fid in values if fid != skip
            ]
        lo, hi = self._range_bounds(ids, op_kind, value)
        return [self._open_value(values[i][1]) for i in range(lo, hi)]

    def value_files_with_values(self) -> List[Tuple[Any, BitmapFile]]:
        """分面枚举：(业务取值, 位图文件)，按业务取值稳定排序。"""
        if self.col_type == "bool":
            return [
                (False, self._open_value(0)),
                (True, self._open_value(1)),
            ]
        if self.col_type == "int64":
            return [
                (v, self._open_value(fid))
                for v, fid in self._load_int_index()
            ]
        # text：扫描哈希目录，dict_id 经列存字典反查字符串
        hd = self._text_dir()
        out: List[Tuple[str, int]] = []
        with open(hd.path, "rb") as fh:
            data = fh.read()
        for i in range(self._text_buckets):
            hh, dict_id, file_id = _QII.unpack_from(data, i * _QII.size)
            if hh != 0:
                out.append((dict_id, file_id))
        result = []
        assert self._col_reader is not None, "text posting 必须注入 ColumnReader"
        for dict_id, file_id in sorted(out):
            value = self._col_reader._read_dict(dict_id)
            result.append((value, self._open_value(file_id)))
        result.sort(key=lambda x: x[0])
        return result

    @staticmethod
    def _bisect(ids: List[int], target: int) -> int:
        lo, hi = 0, len(ids)
        while lo < hi:
            mid = (lo + hi) // 2
            if ids[mid] < target:
                lo = mid + 1
            else:
                hi = mid
        return lo

    @staticmethod
    def _range_bounds(ids: List[int], op_kind: str, value) -> Tuple[int, int]:
        n = len(ids)
        if op_kind == "lt":
            return 0, AttrPosting._bisect(ids, value)
        if op_kind == "le":
            idx = AttrPosting._bisect(ids, value)
            if idx < n and ids[idx] == value:
                idx += 1
            return 0, idx
        if op_kind == "gt":
            idx = AttrPosting._bisect(ids, value)
            if idx < n and ids[idx] == value:
                idx += 1
            return idx, n
        if op_kind == "ge":
            return AttrPosting._bisect(ids, value), n
        raise QueryError(f"不支持的比较: {op_kind}")

    def close(self) -> None:
        for bf in list(self._open.values()):
            bf.close()
        self._open.clear()
        if self._null_file is not None:
            self._null_file.close()
            self._null_file = None
        if self._text_hash is not None:
            self._text_hash.close()
            self._text_hash = None


class PostingRegistry:
    """按 (domain, attr) 惰性缓存 AttrPosting，查询结束统一关闭。"""

    def __init__(self, store: BitmapStore, index_dir: str,
                 parent_schema: Dict[str, str], child_schema: Dict[str, str],
                 parent_pages: int, child_pages: int,
                 col_reader_factory=None):
        self._store = store
        self._index_dir = index_dir
        self._schemas = {"parent": parent_schema, "child": child_schema}
        self._pages = {"parent": parent_pages, "child": child_pages}
        self._col_reader_factory = col_reader_factory
        self._cache: Dict[Tuple[str, str], AttrPosting] = {}

    def get(self, domain: str, attr: str) -> AttrPosting:
        key = (domain, attr)
        ap = self._cache.get(key)
        if ap is None:
            col_reader = None
            if self._schemas[domain][attr] == "text" and self._col_reader_factory:
                col_reader = self._col_reader_factory(domain, attr)
            ap = AttrPosting(
                self._store,
                os.path.join(self._index_dir, "post", domain, attr),
                self._schemas[domain][attr],
                self._pages[domain],
                col_reader=col_reader,
            )
            self._cache[key] = ap
        return ap

    def close(self) -> None:
        for ap in self._cache.values():
            ap.close()
        self._cache.clear()
