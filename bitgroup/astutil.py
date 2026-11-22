"""查询 AST 校验、规范化与指纹。

AST 节点（JSON 可表达）::

    {"op": "cmp", "domain": "parent"|"child", "attr": "region",
     "op_kind": "eq|ne|lt|le|gt|ge", "value": <literal>}
    {"op": "and"|"or", "args": [node, ...]}
    {"op": "not", "arg": node}
    {"op": "exists", "arg": <child node>}
    {"op": "count_where", "arg": <child node>, "k": <non-neg int>}

字面量允许 int64 / text / bool / null；``null`` 与任何值比较在执行期得
unknown（类型检查仍按声明属性走，避免把 bool 与数字混用）。
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Dict, List, Tuple

from .errors import QueryError
from .types import INT64_MAX, INT64_MIN, ScalarType

CMP_OPS = ("eq", "ne", "lt", "le", "gt", "ge")
_CHILD_ONLY = ("exists", "count_where")


def _is_int64(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and INT64_MIN <= v <= INT64_MAX


def validate_literal(value: Any, attr_type: str, where: str) -> Any:
    if value is None:
        return None
    if attr_type == ScalarType.INT64:
        # bool 是 int 子类，必须显式排除（bool 不等于 1）
        if isinstance(value, bool) or not isinstance(value, int):
            raise QueryError(f"{where}: int64 属性的字面量必须是 int64，得到 {value!r}")
        if not (INT64_MIN <= value <= INT64_MAX):
            raise QueryError(f"{where}: int64 字面量越界: {value}")
    elif attr_type == ScalarType.TEXT:
        if not isinstance(value, str):
            raise QueryError(f"{where}: text 属性的字面量必须是字符串，得到 {value!r}")
    elif attr_type == ScalarType.BOOL:
        if not isinstance(value, bool):
            raise QueryError(f"{where}: bool 属性的字面量必须是 true/false，得到 {value!r}")
    elif attr_type == ScalarType.NULL:
        if value is not None:
            raise QueryError(f"{where}: null 类型属性只能与 null 比较")
    return value


def validate_ast(
    node: Any,
    parent_schema: Dict[str, str],
    child_schema: Dict[str, str],
    *,
    in_child: bool = False,
) -> Dict[str, Any]:
    """原地校验并返回规范化后的节点 dict。"""
    if not isinstance(node, dict) or "op" not in node:
        raise QueryError(f"AST 节点必须是带 op 的对象: {node!r}")
    op = node["op"]
    if op == "cmp":
        domain = node.get("domain")
        if domain not in ("parent", "child"):
            raise QueryError("cmp 节点必须声明 domain 为 parent 或 child")
        attr = node.get("attr")
        if not isinstance(attr, str) or not attr:
            raise QueryError("cmp 节点 attr 必须是非空字符串")
        kind = node.get("op_kind")
        if kind not in CMP_OPS:
            raise QueryError(f"cmp 的 op_kind 必须是 {CMP_OPS} 之一")
        schema = parent_schema if domain == "parent" else child_schema
        if attr not in schema:
            raise QueryError(
                f"{domain} 表没有属性 {attr!r}，可用: {sorted(schema)}"
            )
        if "value" not in node:
            raise QueryError("cmp 节点缺少 value（null 请显式写 null）")
        col_type = schema[attr]
        # text/bool 只支持 eq/ne（null 字面量的三态语义由执行期处理）
        if col_type in (ScalarType.TEXT, ScalarType.BOOL) and kind not in ("eq", "ne"):
            raise QueryError(
                f"{domain}.{attr} 是 {col_type} 类型，只支持 eq/ne，得到 {kind}"
            )
        validate_literal(node["value"], col_type, f"{domain}.{attr}")
        if in_child and domain == "parent":
            raise QueryError("child_predicate 内不允许引用 parent 属性")
        return node
    if op in ("and", "or"):
        args = node.get("args")
        if not isinstance(args, list) or not args:
            raise QueryError(f"{op} 需要非空 args 列表")
        node["args"] = [
            validate_ast(a, parent_schema, child_schema, in_child=in_child)
            for a in args
        ]
        return node
    if op == "not":
        if "arg" not in node:
            raise QueryError("not 节点需要 arg")
        node["arg"] = validate_ast(
            node["arg"], parent_schema, child_schema, in_child=in_child
        )
        return node
    if op in _CHILD_ONLY:
        if in_child:
            raise QueryError(f"{op} 不能嵌套在 child_predicate 内")
        if "arg" not in node:
            raise QueryError(f"{op} 节点需要 child_predicate arg")
        node["arg"] = validate_ast(
            node["arg"], parent_schema, child_schema, in_child=True
        )
        _assert_child_only(node["arg"])
        if op == "count_where":
            k = node.get("k")
            if not isinstance(k, int) or isinstance(k, bool) or k < 0:
                raise QueryError("count_where 的 k 必须是非负整数")
        return node
    raise QueryError(f"未知 AST op: {op!r}")


def _assert_child_only(node: Dict[str, Any]) -> None:
    op = node["op"]
    if op == "cmp":
        if node.get("domain") != "child":
            raise QueryError("child_predicate 只能包含 child 域比较")
        return
    if op in ("and", "or"):
        for a in node["args"]:
            _assert_child_only(a)
        return
    if op == "not":
        _assert_child_only(node["arg"])
        return
    raise QueryError(f"child_predicate 内不允许 {op}")


def canonical_json(node: Dict[str, Any]) -> bytes:
    """规范化序列化：键排序、无空白，作为指纹输入。"""
    return json.dumps(
        node, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def ast_fingerprint(node: Dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(node)).hexdigest()


def make_page_token(
    hmac_key: bytes,
    index_token: str,
    fingerprint: str,
    state: str,
    offset: int,
    page_size: int,
) -> str:
    """分页令牌 = 偏移 + HMAC(索引代号, AST 指纹, 状态, 偏移, 页大小)。"""
    payload = {
        "index": index_token,
        "fp": fingerprint,
        "state": state,
        "offset": offset,
        "page_size": page_size,
    }
    body = canonical_json(payload)
    mac = hmac.new(hmac_key, body, hashlib.sha256).hexdigest()
    return body.hex() + "." + mac


def verify_page_token(
    token: str,
    hmac_key: bytes,
    index_token: str,
    fingerprint: str,
    state: str,
) -> Tuple[int, int]:
    """校验令牌绑定关系，返回 (offset, page_size)。"""
    from .errors import TokenError

    try:
        body_hex, mac = token.split(".", 1)
        body = bytes.fromhex(body_hex)
    except (ValueError, AttributeError) as exc:
        raise TokenError("分页令牌格式非法") from exc
    expect = hmac.new(hmac_key, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expect, mac):
        raise TokenError("分页令牌签名不匹配（可能属于其他索引或查询）")
    try:
        payload = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise TokenError("分页令牌内容损坏") from exc
    if payload.get("index") != index_token:
        raise TokenError("分页令牌属于其他索引代号，已拒绝")
    if payload.get("fp") != fingerprint:
        raise TokenError("分页令牌属于其他查询 AST，已拒绝")
    if payload.get("state") != state:
        raise TokenError("分页令牌属于其他结果状态，已拒绝")
    return int(payload["offset"]), int(payload["page_size"])


def iter_cmp_nodes(node: Dict[str, Any]) -> List[Dict[str, Any]]:
    """枚举 AST 内全部 cmp 节点（执行器据此决定打开哪些 posting）。"""
    op = node["op"]
    if op == "cmp":
        return [node]
    if op in ("and", "or"):
        out: List[Dict[str, Any]] = []
        for a in node["args"]:
            out.extend(iter_cmp_nodes(a))
        return out
    if op == "not":
        return iter_cmp_nodes(node["arg"])
    if op in _CHILD_ONLY:
        return iter_cmp_nodes(node["arg"])
    raise QueryError(f"未知 AST op: {op!r}")
