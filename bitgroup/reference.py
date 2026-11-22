"""独立小对象逐行解释器（参考实现）。

刻意不 import bitgroup 的任何模块：直接读原始 JSONL，用 Python 原生集合/
循环逐行计算每个 AST 节点的三态，用来交叉核对产品位图引擎。

三态表示：'true' / 'false' / 'unknown'。Kleene 规则：

* NOT: true<->false，unknown 保持；
* AND: 有 false 即 false，否则有 unknown 即 unknown，否则 true；
* OR : 有 true 即 true，否则有 unknown 即 unknown，否则 false；
* 比较任一边为 null -> unknown；
* exists/count_where 先在子行层算谓词，再按 entity 分组，绝不跨行拼接。
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

T = "true"
F = "false"
U = "unknown"


def _read_jsonl(path: str) -> List[dict]:
    rows = []
    if path is None:
        return rows
    with open(path, "rb") as fh:
        for raw in fh:
            raw = raw.strip()
            if raw:
                rows.append(json.loads(raw))
    return rows


def _not(state: str) -> str:
    return {T: F, F: T, U: U}[state]


def _and(states: List[str]) -> str:
    if F in states:
        return F
    if U in states:
        return U
    return T


def _or(states: List[str]) -> str:
    if T in states:
        return T
    if U in states:
        return U
    return F


def _scalar_compare(actual, op_kind: str, literal) -> bool:
    # 调用方已保证 actual 非 null
    if isinstance(actual, bool) or isinstance(literal, bool):
        if op_kind == "eq":
            return type(actual) is type(literal) and actual == literal
        if op_kind == "ne":
            return not (type(actual) is type(literal) and actual == literal)
        raise ValueError("bool 只支持 eq/ne")
    if isinstance(actual, str) or isinstance(literal, str):
        if op_kind == "eq":
            return actual == literal
        if op_kind == "ne":
            return actual != literal
        raise ValueError("text 只支持 eq/ne")
    return {
        "eq": actual == literal,
        "ne": actual != literal,
        "lt": actual < literal,
        "le": actual <= literal,
        "gt": actual > literal,
        "ge": actual >= literal,
    }[op_kind]


class ReferenceInterpreter:
    """加载主/子 JSONL 后，对任意 AST 给出逐对象三态与逐节点解释。"""

    def __init__(self, parent_path: str, child_path: Optional[str]):
        self.parents = _read_jsonl(parent_path)
        self.children = _read_jsonl(child_path)
        self._by_entity: Dict[str, List[dict]] = {}
        for ch in self.children:
            self._by_entity.setdefault(ch["entity_id"], []).append(ch)
        self.entity_ids = [p["entity_id"] for p in self.parents]

    # ------------------------------------------------------------------

    def _eval_cmp_on_row(self, node: dict, row: dict) -> str:
        attr = node["attr"]
        value = row.get(attr)
        literal = node["value"]
        # 任一边为 null（含显式 null 字面量）-> unknown
        if value is None or literal is None:
            return U
        # bool 与数字严格区分：bool 不等于 1，跨类型比较直接报错
        if isinstance(value, bool) != isinstance(literal, bool):
            raise ValueError(
                f"类型不匹配: {type(value).__name__} 与 {type(literal).__name__} 比较"
            )
        if isinstance(value, str) != isinstance(literal, str):
            raise ValueError("text 与非 text 比较类型不匹配")
        return T if _scalar_compare(value, node["op_kind"], literal) else F

    def eval_node_on_row(self, node: dict, row: dict) -> str:
        op = node["op"]
        if op == "cmp":
            return self._eval_cmp_on_row(node, row)
        if op == "and":
            return _and([self.eval_node_on_row(a, row) for a in node["args"]])
        if op == "or":
            return _or([self.eval_node_on_row(a, row) for a in node["args"]])
        if op == "not":
            return _not(self.eval_node_on_row(node["arg"], row))
        raise ValueError(f"子行谓词内不允许 {op}")

    def _lift(self, node: dict, entity_id: str) -> Tuple[str, int, int, List[dict]]:
        pred = node["arg"]
        kids = self._by_entity.get(entity_id, [])
        traces = []
        true_n = unknown_n = 0
        witness_t = witness_u = None
        for ch in kids:
            st = self.eval_node_on_row(pred, ch)
            traces.append({"child_id": ch["child_id"], "state": st})
            if st == T:
                true_n += 1
                witness_t = witness_t or ch["child_id"]
            elif st == U:
                unknown_n += 1
                witness_u = witness_u or ch["child_id"]
        lb, ub = true_n, true_n + unknown_n
        if node["op"] == "exists":
            if lb >= 1:
                state = T
            elif ub >= 1:
                state = U
            else:
                state = F
        else:
            k = node["k"]
            if lb >= k:
                state = T
            elif ub < k:
                state = F
            else:
                state = U
        return state, lb, ub, traces

    # ------------------------------------------------------------------

    def evaluate_parent(self, node: dict, entity_id: str) -> str:
        """单个父对象上的根节点三态。"""
        op = node["op"]
        parent_row = next(p for p in self.parents if p["entity_id"] == entity_id)
        if op == "cmp":
            assert node["domain"] == "parent"
            return self._eval_cmp_on_row(node, parent_row)
        if op == "and":
            return _and([self.evaluate_parent(a, entity_id) for a in node["args"]])
        if op == "or":
            return _or([self.evaluate_parent(a, entity_id) for a in node["args"]])
        if op == "not":
            return _not(self.evaluate_parent(node["arg"], entity_id))
        if op in ("exists", "count_where"):
            state, _lb, _ub, _tr = self._lift(node, entity_id)
            return state
        raise ValueError(f"未知 op {op}")

    def evaluate_all(self, node: dict) -> Dict[str, List[str]]:
        """返回三态各自的 entity_id 列表（稳定顺序）。"""
        out = {T: [], F: [], U: []}
        for eid in self.entity_ids:
            out[self.evaluate_parent(node, eid)].append(eid)
        return out

    def explain(self, node: dict, entity_id: str) -> dict:
        """逐节点解释（含子行上下界与见证），结构与产品 explain 对齐关键字段。"""
        parent_row = next(p for p in self.parents if p["entity_id"] == entity_id)
        return self._explain(node, entity_id, parent_row)

    def _explain(self, node: dict, entity_id: str, parent_row: dict) -> dict:
        op = node["op"]
        if op == "cmp":
            if node["domain"] == "parent":
                actual = parent_row.get(node["attr"])
                state = U if actual is None or node["value"] is None else (
                    T if _scalar_compare(actual, node["op_kind"], node["value"]) else F
                )
                return {"op": "cmp", "state": state, "actual": actual,
                        "null_kind": "null" if actual is None else None}
            raise AssertionError("child cmp 走子行解释")
        if op in ("and", "or"):
            args = [self._explain(a, entity_id, parent_row) for a in node["args"]]
            states = [a["state"] for a in args]
            return {"op": op, "state": _and(states) if op == "and" else _or(states),
                    "args": args}
        if op == "not":
            arg = self._explain(node["arg"], entity_id, parent_row)
            return {"op": "not", "state": _not(arg["state"]), "arg": arg}
        if op in ("exists", "count_where"):
            state, lb, ub, traces = self._lift(node, entity_id)
            kids = self._by_entity.get(entity_id, [])
            return {
                "op": op,
                "k": node.get("k"),
                "state": state,
                "lower_bound": lb,
                "upper_bound": ub,
                "no_children": len(kids) == 0,
                "children": traces,
            }
        raise ValueError(f"未知 op {op}")

    def facets(self, node: dict, attr: str) -> Dict[str, Any]:
        """分面：true 集合内按主属性取值计数，null 单列，对象不重复。"""
        values: Dict[Any, int] = {}
        null_count = 0
        for p in self.parents:
            if self.evaluate_parent(node, p["entity_id"]) != T:
                continue
            v = p.get(attr)
            if v is None:
                null_count += 1
            else:
                values[v] = values.get(v, 0) + 1
        return {"values": dict(sorted(values.items(), key=lambda kv: str(kv[0]))),
                "null_count": null_count}
