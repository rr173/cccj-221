"""三态集合执行引擎。

一个三态集合（:class:`Trits`）是同一编号域上的两张临时位图 ``t`` 与
``u``：``t`` 置位 = true，``u`` 置位 = unknown，都不置位 = false
（两张图互斥）。所有布尔运算按 Kleene 三值在页粒度逐页计算，中间集合
全部走 :class:`TempBitmap`，受 LRU 页预算约束并计入峰值观测。

相关子条件（exists / count_where）先在 *子行域* 把 child_predicate 算成
三态，再按 ``child_range`` 提升到父对象域，绝不跨子行拼接条件。
``not`` 的补集始终以当前索引的完整主对象域为准（无子女对象同样在内）。
"""

from __future__ import annotations

import json
import os
import struct
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple

from . import astutil
from .astutil import make_page_token, verify_page_token
from .column import ColumnReader
from .errors import IndexError as BgIndexError
from .errors import QueryError, TokenError
from .ident import IdentityStore
from .iomon import IOMonitor
from .pages import (
    PAGE_BITS,
    BitmapStore,
    TempBitmap,
)
from .postings import PostingRegistry

_TRUE = "true"
_FALSE = "false"
_UNKNOWN = "unknown"
_STATES = (_TRUE, _FALSE, _UNKNOWN)
_FULL64 = 0xFFFFFFFFFFFFFFFF
_QII = struct.Struct("<QII")


@dataclass
class Trits:
    """三态集合：t 与 u 互斥，其余位即 false。"""

    t: TempBitmap
    u: TempBitmap
    n_pages: int
    label: str = ""

    def release(self) -> None:
        self.t.release()
        self.u.release()


@dataclass
class QueryResult:
    counts: Dict[str, int]
    page_entity_ids: List[str]
    next_token: Optional[str]
    state: str
    fingerprint: str
    io: Dict[str, int] = field(default_factory=dict)


def _page_bits(p: int, total: int) -> int:
    return min(PAGE_BITS, total - p * PAGE_BITS)


class Engine:
    """绑定一个只读索引代号，可反复执行查询。"""

    def __init__(
        self,
        index_dir: str,
        *,
        cache_budget_bytes: int = 32 * 1024 * 1024,
    ):
        self.index_dir = index_dir
        meta_path = os.path.join(index_dir, "meta.json")
        if not os.path.exists(meta_path):
            raise BgIndexError(f"索引目录缺少 meta.json: {index_dir}")
        with open(meta_path, "r", encoding="utf-8") as fh:
            self.meta = json.load(fh)
        self.index_token = self.meta["index_token"]
        self._hmac = bytes.fromhex(self.meta["hmac_key_hex"])
        self.parent_schema: Dict[str, str] = self.meta["parent_schema"]
        self.child_schema: Dict[str, str] = self.meta["child_schema"]
        self.n_parents = self.meta["n_parents"]
        self.n_children = self.meta["n_children"]
        self.parent_pages = self.meta["parent_pages"]
        self.child_pages = self.meta["child_pages"]
        self.monitor = IOMonitor()
        self.store = BitmapStore(index_dir, self.monitor, cache_budget_bytes)
        self.store.parent_n_pages = self.parent_pages
        self.store.child_n_pages = self.child_pages
        self.ident = IdentityStore(index_dir)
        self.ident.open(
            self.n_parents,
            self.n_children,
            self.meta["parent_buckets"],
            self.meta["child_buckets"],
        )
        self.registry = PostingRegistry(
            self.store,
            index_dir,
            self.parent_schema,
            self.child_schema,
            self.parent_pages,
            self.child_pages,
            col_reader_factory=self._get_col_reader,
        )
        self._col_readers: Dict[Tuple[str, str], ColumnReader] = {}
        self._result_cache: Dict[str, Trits] = {}

    # ------------------------------------------------------------------

    def close(self) -> None:
        for trits in self._result_cache.values():
            trits.release()
        self._result_cache.clear()
        self.registry.close()
        for cr in self._col_readers.values():
            cr.close()
        self._col_readers.clear()
        self.ident.close()
        self.store.close()

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------
    # 查询入口
    # ------------------------------------------------------------------

    def _prepare(self, ast: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        node = astutil.validate_ast(ast, self.parent_schema, self.child_schema)
        fp = astutil.ast_fingerprint(node)
        return node, fp

    def query(
        self,
        ast: Dict[str, Any],
        *,
        state: str = _TRUE,
        page_size: int = 100,
        page_token: Optional[str] = None,
    ) -> QueryResult:
        if state not in _STATES:
            raise QueryError(f"state 必须是 {_STATES} 之一")
        if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size <= 0:
            raise QueryError("page_size 必须是正整数")
        node, fp = self._prepare(ast)
        self.monitor.reset_peaks()
        io_before = self.monitor.snapshot()
        trits = self._eval_root(node, fp)
        counts = self._count_trits(trits)
        offset = 0
        if page_token is not None:
            offset, tok_size = verify_page_token(
                page_token, self._hmac, self.index_token, fp, state
            )
            if tok_size != page_size:
                raise TokenError("分页令牌的 page_size 与本次请求不一致")
            if offset > counts[state]:
                raise TokenError("分页偏移超出结果集合大小")
        ids, next_offset = self._page_ids(trits, state, offset, page_size)
        next_token = None
        if next_offset < counts[state]:
            next_token = make_page_token(
                self._hmac, self.index_token, fp, state,
                next_offset, page_size,
            )
        io = self.monitor.delta(io_before)
        return QueryResult(
            counts=counts,
            page_entity_ids=ids,
            next_token=next_token,
            state=state,
            fingerprint=fp,
            io=io,
        )

    def counts(self, ast: Dict[str, Any]) -> Dict[str, Any]:
        node, fp = self._prepare(ast)
        self.monitor.reset_peaks()
        before = self.monitor.snapshot()
        trits = self._eval_root(node, fp)
        return {
            "counts": self._count_trits(trits),
            "fingerprint": fp,
            "io": self.monitor.delta(before),
        }

    def iter_states(self, ast: Dict[str, Any]) -> Iterator[Tuple[str, str]]:
        """按索引内局部编号升序，流式产出 ``(entity_id, state)``。

        覆盖当前索引完整主对象域：``true`` / ``false`` / ``unknown`` 三态
        各恰好出现一次。身份翻译与位图读取都是顺序流，不把全日结果物化成
        Python 列表；供历史台账逐日登记使用。
        """
        node, fp = self._prepare(ast)
        trits = self._eval_root(node, fp)
        ids = self.ident.iter_parent_ids()
        for p in range(self.n_parents):
            tid = trits.t.get_bit(p)
            if tid:
                state = _TRUE
            elif trits.u.get_bit(p):
                state = _UNKNOWN
            else:
                state = _FALSE
            entity_id = next(ids)
            yield entity_id, state
        for _ in ids:
            raise AssertionError("父身份流长度超过 n_parents")

    # ------------------------------------------------------------------
    # 求值骨架
    # ------------------------------------------------------------------

    def _eval_root(self, node: Dict[str, Any], fp: str) -> Trits:
        cached = self._result_cache.get(fp)
        if cached is not None:
            return cached
        trits = self._eval(node, domain="parent")
        self._result_cache[fp] = trits
        return trits

    def _new_trits(self, n_pages: int, label: str) -> Trits:
        return Trits(
            self.store.new_temp(n_pages, label + ".t"),
            self.store.new_temp(n_pages, label + ".u"),
            n_pages,
            label,
        )

    def _domain_total(self, domain: str) -> int:
        return self.n_parents if domain == "parent" else self.n_children

    def _eval(self, node: Dict[str, Any], *, domain: str) -> Trits:
        op = node["op"]
        n_pages = self.parent_pages if domain == "parent" else self.child_pages
        if op == "cmp":
            return self._eval_cmp(node, n_pages)
        if op == "and":
            return self._eval_bool(node, domain, is_and=True)
        if op == "or":
            return self._eval_bool(node, domain, is_and=False)
        if op == "not":
            return self._eval_not(node, domain, n_pages)
        if op in ("exists", "count_where"):
            return self._eval_lift(node)
        raise QueryError(f"未知 AST op: {op}")

    def _eval_cmp(self, node: Dict[str, Any], n_pages: int) -> Trits:
        out = self._new_trits(n_pages, f"cmp:{node['domain']}.{node['attr']}")
        ap = self.registry.get(node["domain"], node["attr"])
        total = self._domain_total(node["domain"])
        # 字面量为 null：比较任一边 null 即 unknown —— 整个域全部 unknown，
        # 没有 true 也没有 false（无论该行取值是否为 null）。
        if node["value"] is None:
            for p in range(n_pages):
                out.u.fill_page(p, _page_bits(p, total))
            return out
        null_bf = ap.open_null()
        true_files = ap.compare_files(node["op_kind"], node["value"])
        for p in range(n_pages):
            nw = null_bf.read_page(p)
            if nw is not None:
                out.u.or_page_bits(p, nw)
            for bf in true_files:
                tw = bf.read_page(p)
                if tw is not None:
                    out.t.or_page_bits(p, tw)
        return out

    def _eval_bool(self, node: Dict[str, Any], domain: str, *, is_and: bool) -> Trits:
        children = [self._eval(a, domain=domain) for a in node["args"]]
        n_pages = children[0].n_pages
        total = self._domain_total(domain)
        out = self._new_trits(n_pages, "and" if is_and else "or")
        try:
            for p in range(n_pages):
                valid = _page_bits(p, total)
                ts = [c.t.get_page(p) for c in children]
                us = [c.u.get_page(p) for c in children]
                if is_and:
                    t_page = ts[0]
                    for w in ts[1:]:
                        t_page = BitmapStore.page_and(t_page, w)
                    u_page = self._and_unknown(ts, us, valid)
                else:
                    t_page = ts[0]
                    for w in ts[1:]:
                        t_page = BitmapStore.page_or(t_page, w)
                    u_page = self._or_unknown(ts, us, valid)
                if t_page is not None and BitmapStore.page_any(t_page):
                    out.t.or_page_bits(p, t_page)
                if u_page is not None and BitmapStore.page_any(u_page):
                    out.u.or_page_bits(p, u_page)
        finally:
            for c in children:
                c.release()
        return out

    @staticmethod
    def _and_unknown(ts, us, valid: int):
        # Kleene AND unknown：至少一个 u，且没有任何子节点为 false。
        # 某子节点为 false 的位 = NOT(t OR u)（页内全字域，稍后按 valid 掩码）。
        any_false = None
        for t, u in zip(ts, us):
            f = BitmapStore.page_complement(BitmapStore.page_or(t, u), PAGE_BITS)
            any_false = f if any_false is None else BitmapStore.page_or(any_false, f)
        any_u = us[0]
        for w in us[1:]:
            any_u = BitmapStore.page_or(any_u, w)
        # unknown = any_u AND NOT any_false
        page = BitmapStore.page_andnot(any_u, any_false)
        return Engine._mask_valid(page, valid)

    @staticmethod
    def _or_unknown(ts, us, valid: int):
        # Kleene OR unknown：至少一个 u，且没有任何子节点为 true。
        any_true = ts[0]
        for w in ts[1:]:
            any_true = BitmapStore.page_or(any_true, w)
        any_u = us[0]
        for w in us[1:]:
            any_u = BitmapStore.page_or(any_u, w)
        page = BitmapStore.page_andnot(any_u, any_true)
        return Engine._mask_valid(page, valid)

    @staticmethod
    def _mask_valid(page, valid: int):
        if page is None:
            return None
        full, rem = divmod(valid, 64)
        if full < 1024:
            page[full] &= (1 << rem) - 1 if rem else 0
            for i in range(full + 1, 1024):
                page[i] = 0
        return page

    def _eval_not(self, node: Dict[str, Any], domain: str, n_pages: int) -> Trits:
        inner = self._eval(node["arg"], domain=domain)
        total = self._domain_total(domain)
        out = self._new_trits(n_pages, "not")
        try:
            for p in range(n_pages):
                valid = _page_bits(p, total)
                known = BitmapStore.page_or(
                    inner.t.get_page(p), inner.u.get_page(p)
                )
                new_t = BitmapStore.page_complement(known, valid)
                if BitmapStore.page_any(new_t):
                    out.t.or_page_bits(p, new_t)
                u_words = inner.u.get_page(p)
                if u_words is not None:
                    out.u.or_page_bits(p, u_words)
        finally:
            inner.release()
        return out

    # ------------------------------------------------------------------
    # 子行提升：先子行域三态，再按 child_range 分组聚合
    # ------------------------------------------------------------------

    def _eval_lift(self, node: Dict[str, Any]) -> Trits:
        child_trits = self._eval(node["arg"], domain="child")
        out = self._new_trits(self.parent_pages, node["op"])
        is_count = node["op"] == "count_where"
        k = node.get("k", 0)
        try:
            # 流式顺序扫描 mmap range 表，不构造 O(n_parents) 列表
            for pid, start, end in self.ident.ranges.iter_nonempty():
                lb, ub = self._count_group(child_trits, start, end)
                self.monitor.child_rows_processed += end - start
                state = self._lift_state(is_count, k, lb, ub)
                if state == _TRUE:
                    out.t.set_bit(pid)
                elif state == _UNKNOWN:
                    out.u.set_bit(pid)
            self._fill_childless(out, is_count, k)
        finally:
            child_trits.release()
        return out

    @staticmethod
    def _lift_state(is_count: bool, k: int, lb: int, ub: int) -> str:
        if is_count:
            if lb >= k:
                return _TRUE
            if ub < k:
                return _FALSE
            return _UNKNOWN
        if lb >= 1:
            return _TRUE
        if ub >= 1:
            return _UNKNOWN
        return _FALSE

    def _count_group(self, child_trits: Trits, start: int, end: int) -> Tuple[int, int]:
        true_n = unknown_n = 0
        # 页内批量 popcount，避免逐位 get_bit 的缓存抖动
        first_page = start // PAGE_BITS
        last_page = (end - 1) // PAGE_BITS
        for p in range(first_page, last_page + 1):
            lo = max(start, p * PAGE_BITS)
            hi = min(end, (p + 1) * PAGE_BITS)
            tw = child_trits.t.get_page(p)
            uw = child_trits.u.get_page(p)
            true_n += self._range_popcount(tw, lo - p * PAGE_BITS, hi - p * PAGE_BITS)
            unknown_n += self._range_popcount(uw, lo - p * PAGE_BITS, hi - p * PAGE_BITS)
        return true_n, true_n + unknown_n

    @staticmethod
    def _range_popcount(words, lo_bit: int, hi_bit: int) -> int:
        """统计页内 [lo_bit, hi_bit) 的置位数。"""
        if words is None:
            return 0
        total = 0
        wi = lo_bit >> 6
        wend = (hi_bit + 63) >> 6
        for w in range(wi, wend):
            word = words[w]
            bit_lo = max(lo_bit, w * 64) - w * 64
            bit_hi = min(hi_bit, (w + 1) * 64) - w * 64
            mask = ((1 << bit_hi) - 1) & ~((1 << bit_lo) - 1)
            total += (word & mask).bit_count()
        return total

    def _fill_childless(self, out: Trits, is_count: bool, k: int) -> None:
        # 与 iter_nonempty 归并：range 表按 pid 升序，线性扫描找无子女 pid，
        # 不建大 set。
        nonempty = self.ident.ranges.iter_nonempty()
        nxt = next(nonempty, None)
        for pid in range(self.n_parents):
            if nxt is not None and nxt[0] == pid:
                nxt = next(nonempty, None)
                continue
            if is_count and k == 0:
                out.t.set_bit(pid)
            # exists 或 k>0：无子女即 false，不置位

    # ------------------------------------------------------------------
    # 计数 / 分页
    # ------------------------------------------------------------------

    def _count_trits(self, trits: Trits) -> Dict[str, int]:
        t = u = 0
        for p in range(trits.n_pages):
            tp = trits.t.get_page(p)
            up = trits.u.get_page(p)
            if BitmapStore.page_popcount(BitmapStore.page_and(tp, up)) > 0:
                raise AssertionError("三态位图互斥不变量被破坏")
            t += BitmapStore.page_popcount(tp)
            u += BitmapStore.page_popcount(up)
        return {_TRUE: t, _UNKNOWN: u, _FALSE: self.n_parents - t - u}

    def _page_ids(
        self, trits: Trits, state: str, offset: int, page_size: int
    ) -> Tuple[List[str], int]:
        ids: List[str] = []
        if state == _FALSE:
            skipped = 0
            emitted = 0
            for pid in range(self.n_parents):
                if trits.t.get_bit(pid) or trits.u.get_bit(pid):
                    continue
                if skipped < offset:
                    skipped += 1
                    continue
                ids.append(self.ident.entity_id_at(pid))
                emitted += 1
                if emitted == page_size:
                    break
            return ids, offset + emitted
        bitmap = trits.t if state == _TRUE else trits.u
        skipped = 0
        emitted = 0
        for local in BitmapStore.iter_set_bits(bitmap, self.n_parents):
            if skipped < offset:
                skipped += 1
                continue
            ids.append(self.ident.entity_id_at(local))
            emitted += 1
            if emitted == page_size:
                break
        return ids, offset + emitted

    # ------------------------------------------------------------------
    # 分面：同一对象只计一次（对象级 true 集合与取值位图求交）
    # ------------------------------------------------------------------

    def facets(self, ast: Dict[str, Any], attr: str) -> Dict[str, Any]:
        if attr not in self.parent_schema:
            raise QueryError(f"主表没有属性 {attr!r}")
        node, fp = self._prepare(ast)
        self.monitor.reset_peaks()
        before = self.monitor.snapshot()
        trits = self._eval_root(node, fp)
        typ = self.parent_schema[attr]
        ap = self.registry.get("parent", attr)
        null_bf = ap.open_null()
        null_count = 0
        for p in range(self.parent_pages):
            tw = trits.t.get_page(p)
            if tw is None:
                continue
            nw = null_bf.read_page(p)
            if nw is not None:
                null_count += BitmapStore.page_popcount(
                    BitmapStore.page_and(tw, nw)
                )
        value_counts: List[Tuple[Any, int]] = []
        for value, bf in ap.value_files_with_values():
            c = 0
            for p in range(self.parent_pages):
                tw = trits.t.get_page(p)
                if tw is None:
                    continue
                vw = bf.read_page(p)
                if vw is not None:
                    c += BitmapStore.page_popcount(BitmapStore.page_and(tw, vw))
            if c > 0:  # 分面只列在 true 集合中实际出现的取值
                value_counts.append((value, c))
        return {
            "attr": attr,
            "type": typ,
            "values": [{"value": v, "count": c} for v, c in value_counts],
            "null_count": null_count,
            "io": self.monitor.delta(before),
        }

    # ------------------------------------------------------------------
    # explain-entity
    # ------------------------------------------------------------------

    def explain_entity(self, ast: Dict[str, Any], entity_id: str) -> Dict[str, Any]:
        node, fp = self._prepare(ast)
        pid = self.ident.pid_of(entity_id)
        if pid is None:
            raise QueryError(f"未知 entity_id: {entity_id!r}")
        self.monitor.reset_peaks()
        before = self.monitor.snapshot()
        trace = self._explain_node(node, pid, domain="parent")
        return {
            "entity_id": entity_id,
            "pid": pid,
            "fingerprint": fp,
            "root": trace,
            "io": self.monitor.delta(before),
        }

    def _explain_node(self, node: Dict[str, Any], local_id: int, *, domain: str) -> Dict[str, Any]:
        op = node["op"]
        if op == "cmp":
            return self._explain_cmp(node, local_id)
        if op in ("and", "or"):
            args = [self._explain_node(a, local_id, domain=domain) for a in node["args"]]
            state = kleene_combine_states(op, [a["state"] for a in args])
            return {"op": op, "state": state, "args": args}
        if op == "not":
            arg = self._explain_node(node["arg"], local_id, domain=domain)
            return {"op": "not", "state": kleene_not_state(arg["state"]), "arg": arg}
        if op in ("exists", "count_where"):
            return self._explain_lift(node, local_id)
        raise QueryError(f"未知 AST op: {op}")

    def _explain_cmp(self, node: Dict[str, Any], local_id: int) -> Dict[str, Any]:
        domain = node["domain"]
        attr = node["attr"]
        reader = self._get_col_reader(domain, attr)
        value = reader.read(local_id)
        if value is None:
            state = _UNKNOWN
            null_kind = "null"
        else:
            state = _TRUE if scalar_compare(value, node["op_kind"], node["value"]) \
                else _FALSE
            null_kind = None
        return {
            "op": "cmp",
            "domain": domain,
            "attr": attr,
            "op_kind": node["op_kind"],
            "literal": node["value"],
            "actual": value,
            "state": state,
            "null_kind": null_kind,
        }

    def _explain_lift(self, node: Dict[str, Any], pid: int) -> Dict[str, Any]:
        start, end = self.ident.child_range(pid)
        no_children = start == end
        child_traces = []
        true_n = unknown_n = 0
        witness_true = witness_unknown = None
        for cid in range(start, end):
            tr = self._explain_node(node["arg"], cid, domain="child")
            child_id = self.ident.child_id_at(cid)
            tr["child_id"] = child_id
            tr["cid"] = cid
            child_traces.append(tr)
            if tr["state"] == _TRUE:
                true_n += 1
                witness_true = witness_true or child_id
            elif tr["state"] == _UNKNOWN:
                unknown_n += 1
                witness_unknown = witness_unknown or child_id
        lb, ub = true_n, true_n + unknown_n
        state = self._lift_state(
            node["op"] == "count_where", node.get("k", 0), lb, ub
        )
        return {
            "op": node["op"],
            "k": node.get("k"),
            "state": state,
            "lower_bound": lb,
            "upper_bound": ub,
            "no_children": no_children,
            "witnesses": {"true": witness_true, "unknown": witness_unknown},
            "children": child_traces,
        }

    def _get_col_reader(self, domain: str, attr: str) -> ColumnReader:
        key = (domain, attr)
        cr = self._col_readers.get(key)
        if cr is None:
            typ = (self.parent_schema if domain == "parent"
                   else self.child_schema)[attr]
            base = os.path.join(self.index_dir, "col", domain, attr)
            cr = ColumnReader(base, typ)
            self._col_readers[key] = cr
        return cr


# --------------------------------------------------------------------------
# 标量比较与三态工具
# --------------------------------------------------------------------------


def scalar_compare(actual, op_kind: str, literal) -> bool:
    """actual 已非 null；literal 为 null 时恒 false（unknown 由调用方处理）。"""
    if literal is None:
        return False
    if isinstance(actual, bool) or isinstance(literal, bool):
        if op_kind == "eq":
            return actual == literal and isinstance(actual, bool) == isinstance(literal, bool)
        if op_kind == "ne":
            return not (actual == literal and isinstance(actual, bool) == isinstance(literal, bool))
        raise QueryError("bool 只支持 eq/ne")
    if isinstance(actual, str) or isinstance(literal, str):
        if op_kind == "eq":
            return actual == literal
        if op_kind == "ne":
            return actual != literal
        raise QueryError("text 只支持 eq/ne")
    if op_kind == "eq":
        return actual == literal
    if op_kind == "ne":
        return actual != literal
    if op_kind == "lt":
        return actual < literal
    if op_kind == "le":
        return actual <= literal
    if op_kind == "gt":
        return actual > literal
    if op_kind == "ge":
        return actual >= literal
    raise QueryError(f"不支持的比较: {op_kind}")


def kleene_not_state(state: str) -> str:
    if state == _TRUE:
        return _FALSE
    if state == _FALSE:
        return _TRUE
    return _UNKNOWN


def kleene_combine_states(op: str, states: List[str]) -> str:
    if op == "and":
        if any(s == _FALSE for s in states):
            return _FALSE
        if any(s == _UNKNOWN for s in states):
            return _UNKNOWN
        return _TRUE
    if any(s == _TRUE for s in states):
        return _TRUE
    if any(s == _UNKNOWN for s in states):
        return _UNKNOWN
    return _FALSE
