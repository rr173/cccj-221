"""JSONL 导入器：主表/子表 -> 只读磁盘索引。

流程
====

1. **主表**：流式读 JSONL，校验 schema 与 entity_id 唯一性，构建身份哈希
   目录、pid 顺序 blob 与父域各列列存；entity 输入顺序即 pid 顺序（这是
   显式的身份映射，不是“靠顺序猜父子”）。
2. **子表分区**：流式读子表 JSONL，校验 child_id 唯一与 entity_id 悬空，
   按 ``hash(entity_id) % P`` 外部哈希分区落盘，任何时刻只缓存一个分区块。
3. **分区处理**：逐分区读回，按 pid 稳定排序，按到达顺序赋 cid（同一
   entity 的子行连续），写 cid 顺序 blob、child_range、子域列存与 posting。
4. **posting 物化**：父/子各属性从列存生成取值位图与取值索引。
5. **meta 与代号**：写 ``meta.json``（含随机索引代号、HMAC 密钥、schema、
   规模与桶数），原 JSONL 之后移走也不影响查询。
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import struct
from typing import Dict, List, Optional, Tuple

from . import column as col
from .errors import InputDataError, SchemaError
from .iomon import IOMonitor
from .ident import (
    ChildRangeTable,
    IdentityStore,
    fnv1a64,
)
from .pages import BitmapStore, shards_for
from .types import ScalarType, validate_scalar

# 分区记录（定长类型化，避免二次 JSON 往返）：
#   头 <QII: entity_hash, pid, 整条记录长度
#   子行身份 <I 长度 + utf8 child_id
#   按 schema 顺序逐属性：int64 <q? 用标记>；bool/ null/ text 见下
# 统一属性编码：1 字节 tag，随后按类型
#   int64 : tag + <q（null 时仅 tag）
#   bool  : tag（1/0/null 三态编码进 tag）
#   text  : tag + <I 长度 + utf8（空串长度为 0，null 仅 tag）
#   null  : tag（恒 null）
_TAG_VALID = 1
_TAG_NULL = 0
_PART_HEADER = struct.Struct("<QII")
_PART_TARGET_BYTES = 8 * 1024 * 1024
_PART_MAX = 4096


def _encode_attr(typ: str, value, out: bytearray) -> None:
    if value is None:
        out.append(_TAG_NULL)
        return
    out.append(_TAG_VALID)
    if typ == ScalarType.INT64:
        out += struct.pack("<q", value)
    elif typ == ScalarType.BOOL:
        out.append(1 if value else 0)
    elif typ == ScalarType.TEXT:
        b = value.encode("utf-8")
        out += struct.pack("<I", len(b))
        out += b
    elif typ == ScalarType.NULL:
        pass  # 不应出现非 null（validate_scalar 已保证）


def _decode_attr(typ: str, buf: bytes, pos: int):
    tag = buf[pos]
    pos += 1
    if tag == _TAG_NULL:
        return None, pos
    if typ == ScalarType.INT64:
        (v,) = struct.unpack_from("<q", buf, pos)
        return v, pos + 8
    if typ == ScalarType.BOOL:
        return buf[pos] == 1, pos + 1
    if typ == ScalarType.TEXT:
        (ln,) = struct.unpack_from("<I", buf, pos)
        pos += 4
        return buf[pos: pos + ln].decode("utf-8"), pos + ln
    if typ == ScalarType.NULL:
        return None, pos
    raise InputDataError(f"分区记录未知类型 {typ}")


def _read_jsonl(path: str, where: str):
    if not os.path.exists(path):
        raise InputDataError(f"{where}: 文件不存在: {path}")
    with open(path, "rb") as fh:
        for lineno, raw in enumerate(fh, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise InputDataError(
                    f"{where} 第 {lineno} 行不是合法 JSON: {exc.msg}"
                ) from exc
            if not isinstance(obj, dict):
                raise InputDataError(f"{where} 第 {lineno} 行必须是 JSON 对象")
            yield lineno, obj


def _check_schema(schema: Dict, label: str, *, allow_empty: bool = False) -> Dict[str, str]:
    if not isinstance(schema, dict) or (not schema and not allow_empty):
        raise SchemaError(f"{label} schema 必须是对象 attr -> type")
    for attr, typ in schema.items():
        if not isinstance(attr, str) or not attr:
            raise SchemaError(f"{label} schema 属性名非法: {attr!r}")
        if typ not in ScalarType.ALL:
            raise SchemaError(
                f"{label} 属性 {attr!r} 类型 {typ!r} 不在 {ScalarType.ALL}"
            )
    return dict(schema)


class Importer:
    def __init__(
        self,
        index_dir: str,
        parent_schema: Dict[str, str],
        child_schema: Dict[str, str],
        *,
        cache_budget_bytes: int = 32 * 1024 * 1024,
    ):
        self.index_dir = index_dir
        self.parent_schema = _check_schema(parent_schema, "主表")
        self.child_schema = _check_schema(child_schema, "子表", allow_empty=True)
        if "entity_id" in self.parent_schema or "child_id" in self.child_schema \
                or "entity_id" in self.child_schema:
            raise SchemaError("entity_id/child_id 是保留身份字段，不能在 schema 中声明")
        os.makedirs(index_dir, exist_ok=True)
        if os.listdir(index_dir):
            raise SchemaError(f"索引目录非空: {index_dir}")
        self.monitor_store = BitmapStore(index_dir, IOMonitor(), cache_budget_bytes)
        self.ident = IdentityStore(index_dir)
        self._parent_ids: List[str] = []
        self._parent_writers: Dict[str, object] = {}
        self._child_writers: Dict[str, object] = {}
        self.partition_count = 0

    # ------------------------------------------------------------------
    # 阶段 1：主表
    # ------------------------------------------------------------------

    def import_parent(self, parent_jsonl: str) -> int:
        # 先估行数以定哈希桶，再流式写入；重复 entity_id 由哈希目录拒绝
        est = self._estimate_lines(parent_jsonl)
        self.ident.begin_parent_build(est)
        writers = self._open_parent_writers()
        n = 0
        for lineno, obj in _read_jsonl(parent_jsonl, "主表"):
            eid = obj.get("entity_id")
            if not isinstance(eid, str) or not eid:
                raise InputDataError(
                    f"主表第 {lineno} 行: entity_id 必须是非空字符串"
                )
            for attr, typ in self.parent_schema.items():
                if attr not in obj:
                    raise InputDataError(
                        f"主表第 {lineno} 行缺少属性 {attr!r}（null 需显式给 null）"
                    )
            pid = self.ident.add_parent(eid)  # 重复身份在此抛 InputDataError
            for attr, typ in self.parent_schema.items():
                value = validate_scalar(
                    obj[attr], typ, where=f"主表 {eid}.{attr}"
                )
                writers[attr].add(value, pid)
            n += 1
        if n == 0:
            raise InputDataError("主表为空：至少需要一个主对象")
        for w in writers.values():
            w.close()
        return self.ident.finish_parent_build()

    def _open_parent_writers(self) -> Dict[str, object]:
        base = os.path.join(self.index_dir, "col", "parent")
        os.makedirs(base, exist_ok=True)
        out = {}
        for attr, typ in self.parent_schema.items():
            out[attr] = self._make_writer(os.path.join(base, attr), typ)
        self._parent_writers = out
        return out

    @staticmethod
    def _make_writer(base_path: str, typ: str):
        if typ == ScalarType.INT64:
            return col.IntColumnWriter(base_path)
        if typ == ScalarType.BOOL:
            return col.BoolColumnWriter(base_path)
        if typ == ScalarType.TEXT:
            return col.TextColumnWriter(base_path)
        return col.NullColumnWriter(base_path)

    # ------------------------------------------------------------------
    # 阶段 2：子表外部哈希分区
    # ------------------------------------------------------------------

    def partition_children(self, child_jsonl: str) -> Tuple[int, int]:
        """返回 (分区数, 子行数)。"""
        # 先估算行数定分区数（两遍读文件，但只做字节计数，不缓存对象）
        est = self._estimate_lines(child_jsonl)
        self.partition_count = min(
            _PART_MAX, max(1, est * 512 // max(1, _PART_TARGET_BYTES))
        )
        pdir = os.path.join(self.index_dir, "work", "parts")
        os.makedirs(pdir, exist_ok=True)
        parts = [
            open(os.path.join(pdir, f"part_{i}.bin"), "w+b")
            for i in range(self.partition_count)
        ]
        n_children = 0
        try:
            for lineno, obj in _read_jsonl(child_jsonl, "子表"):
                cid_field = obj.get("child_id")
                eid = obj.get("entity_id")
                if not isinstance(cid_field, str) or not cid_field:
                    raise InputDataError(
                        f"子表第 {lineno} 行: child_id 必须是非空字符串"
                    )
                if not isinstance(eid, str) or not eid:
                    raise InputDataError(
                        f"子表第 {lineno} 行: entity_id 必须是非空字符串"
                    )
                pid = self.ident.pid_of(eid)
                if pid is None:
                    raise InputDataError(
                        f"子表第 {lineno} 行悬空 entity_id: {eid!r} "
                        f"(child_id={cid_field!r})"
                    )
                for attr, typ in self.child_schema.items():
                    if attr not in obj:
                        raise InputDataError(
                            f"子表第 {lineno} 行缺少属性 {attr!r}"
                        )
                    validate_scalar(
                        obj[attr], typ,
                        where=f"子表 {cid_field}.{attr}",
                    )
                # 类型化定长编码，省去处理阶段的二次 JSON 解析
                body = bytearray()
                cb = cid_field.encode("utf-8")
                body += struct.pack("<I", len(cb))
                body += cb
                for attr, typ in self.child_schema.items():
                    _encode_attr(typ, obj[attr], body)
                h = fnv1a64(eid.encode("utf-8"))
                part_no = h % self.partition_count
                parts[part_no].write(_PART_HEADER.pack(h, pid, len(body)))
                parts[part_no].write(body)
                n_children += 1
        finally:
            for p in parts:
                p.close()
        return self.partition_count, n_children

    @staticmethod
    def _estimate_lines(path: str) -> int:
        n = 0
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                n += chunk.count(b"\n")
        return max(1, n)

    # ------------------------------------------------------------------
    # 阶段 3：分区处理 -> cid 身份 + 列存 + child_range
    # ------------------------------------------------------------------

    def process_partitions(self, n_parents: int, n_children: int) -> None:
        pdir = os.path.join(self.index_dir, "work", "parts")
        base = os.path.join(self.index_dir, "col", "child")
        os.makedirs(base, exist_ok=True)
        writers = {
            attr: self._make_writer(os.path.join(base, attr), typ)
            for attr, typ in self.child_schema.items()
        }
        # 子身份流式构建；重复 child_id 由哈希目录自身拒绝，无需内存 set
        self.ident.begin_child_build(n_children)
        # range 表：先建空表，用 mmap 按 pid 定长写入（不占常驻 RSS）
        range_table = ChildRangeTable(
            os.path.join(self.ident.dir, "child_range.bin"), n_parents
        )
        range_table.build_empty()
        rfh = open(range_table.path, "r+b")
        import mmap as _mmap
        rmm = _mmap.mmap(rfh.fileno(), 0)
        cid = 0
        try:
            for part_no in range(self.partition_count):
                path = os.path.join(pdir, f"part_{part_no}.bin")
                recs = self._read_partition(path)  # 单分区有界（约 8MiB 目标）
                recs.sort(key=lambda r: r[0])  # 按 pid 稳定排序，保持到达顺序
                i = 0
                while i < len(recs):
                    pid = recs[i][0]
                    j = i
                    while j < len(recs) and recs[j][0] == pid:
                        j += 1
                    start_cid = cid
                    for k in range(i, j):
                        _pid, child_id, values = recs[k]
                        # 重复 child_id 在此抛 InputDataError
                        new_cid = self.ident.add_child(child_id)
                        assert new_cid == cid
                        for attr in self.child_schema:
                            writers[attr].add(values[attr], cid)
                        cid += 1
                    rmm[pid * 16: pid * 16 + 16] = struct.pack(
                        "<QQ", start_cid, j - i
                    )
                    i = j
                del recs
        finally:
            rmm.flush()
            rmm.close()
            rfh.close()
            for w in writers.values():
                w.close()
        actual = self.ident.finish_child_build()
        if actual != n_children:
            raise InputDataError(
                f"子行计数不一致: 分区 {n_children}，实际处理 {actual}"
            )
        self._child_writers = writers
        shutil.rmtree(os.path.join(self.index_dir, "work"), ignore_errors=True)

    def _read_partition(self, path: str) -> List[Tuple[int, str, dict]]:
        out: List[Tuple[int, str, dict]] = []
        with open(path, "rb") as fh:
            data = fh.read()
        pos = 0
        schema_items = list(self.child_schema.items())
        n = len(data)
        while pos < n:
            _h, pid, plen = _PART_HEADER.unpack_from(data, pos)
            pos += _PART_HEADER.size
            end = pos + plen
            (clen,) = struct.unpack_from("<I", data, pos)
            pos += 4
            child_id = data[pos: pos + clen].decode("utf-8")
            pos += clen
            values: Dict[str, object] = {}
            for attr, typ in schema_items:
                v, pos = _decode_attr(typ, data, pos)
                values[attr] = v
            if pos != end:
                raise InputDataError(f"分区记录长度损坏: {path}")
            out.append((pid, child_id, values))
        return out

    # ------------------------------------------------------------------
    # 阶段 4：posting 物化
    # ------------------------------------------------------------------

    def build_postings(self, n_parents: int, n_children: int) -> None:
        post = os.path.join(self.index_dir, "post")
        work = os.path.join(self.index_dir, "work_sort")
        for domain, n_rows, schema in (
            ("parent", n_parents, self.parent_schema),
            ("child", n_children, self.child_schema),
        ):
            pb = col.PostingBuilder(post, domain, n_rows)
            base_root = os.path.join(self.index_dir, "col", domain)
            for attr, typ in schema.items():
                base_path = os.path.join(base_root, attr)
                if n_rows == 0:
                    # 空域：只生成空 null 位图与空取值索引，不依赖列文件
                    pb.build_empty_attr(attr, typ)
                    continue
                if typ == ScalarType.INT64:
                    pb.build_int(attr, base_path,
                                 os.path.join(work, domain, attr))
                elif typ == ScalarType.TEXT:
                    pb.build_text(attr, base_path)
                elif typ == ScalarType.BOOL:
                    pb.build_bool(attr, base_path)
                else:
                    pb.build_null_only(attr, base_path)
        shutil.rmtree(work, ignore_errors=True)

    # ------------------------------------------------------------------
    # 阶段 5：meta / 代号
    # ------------------------------------------------------------------

    def finalize(
        self, n_parents: int, n_children: int
    ) -> "IndexHandle":
        token = secrets.token_hex(16)
        hmac_key = secrets.token_bytes(32)
        meta = {
            "format_version": 1,
            "index_token": token,
            "hmac_key_hex": hmac_key.hex(),
            "parent_schema": self.parent_schema,
            "child_schema": self.child_schema,
            "n_parents": n_parents,
            "n_children": n_children,
            "parent_pages": shards_for(n_parents),
            "child_pages": shards_for(n_children),
            "parent_buckets": self.ident.buckets_for(n_parents),
            "child_buckets": self.ident.buckets_for(n_children),
        }
        tmp = os.path.join(self.index_dir, "meta.json.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, os.path.join(self.index_dir, "meta.json"))
        return IndexHandle(self.index_dir, token, meta)

    def run(self, parent_jsonl: str, child_jsonl: Optional[str]) -> "IndexHandle":
        n_parents = self.import_parent(parent_jsonl)
        n_children = 0
        if child_jsonl is not None:
            _parts, n_children = self.partition_children(child_jsonl)
            self.process_partitions(n_parents, n_children)
            # 子身份目录在 process_partitions 内创建；无子表则不产生任何子文件
        self.build_postings(n_parents, n_children)
        handle = self.finalize(n_parents, n_children)
        self.monitor_store.close()
        return handle


class IndexHandle:
    """导入结果：只读索引代号与元数据。"""

    def __init__(self, index_dir: str, token: str, meta: dict):
        self.index_dir = index_dir
        self.index_token = token
        self.meta = meta

    def __repr__(self) -> str:
        return (
            f"IndexHandle(dir={self.index_dir!r}, token={self.index_token[:8]}…, "
            f"parents={self.meta['n_parents']}, children={self.meta['n_children']})"
        )
