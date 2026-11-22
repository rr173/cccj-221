"""标量类型与校验。

支持四种取值：int64、text、bool、null。注意：

* 空字符串 ``""`` 是有值的 text，不是 null；
* bool 与数字严格区分，``True`` 不等于 ``1``；
* JSON None 即 SQL 意义上的 null。
"""

from __future__ import annotations

from typing import Any

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


class ScalarType:
    INT64 = "int64"
    TEXT = "text"
    BOOL = "bool"
    NULL = "null"
    ALL = ("int64", "text", "bool", "null")


def json_type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, str):
        return "text"
    return type(value).__name__


def validate_scalar(value: Any, declared: str, *, where: str) -> Any:
    """按声明类型校验一个 JSONL 标量，非法时抛出 InputDataError。

    ``null`` 对任何声明类型都合法（缺失值语义）。``null`` 类型的列只允许
    取值 null。返回原值。
    """
    from .errors import InputDataError

    if declared not in ScalarType.ALL:
        from .errors import SchemaError

        raise SchemaError(f"未知标量类型 {declared!r}（{where}）")
    if value is None:
        return None
    if declared == ScalarType.NULL:
        raise InputDataError(
            f"{where}: null 类型列只能取 null，实际得到 {json_type_name(value)}: {value!r}"
        )
    if declared == ScalarType.INT64:
        # bool 是 int 的子类，必须显式排除：bool 不等于 1。
        if isinstance(value, bool) or not isinstance(value, int):
            raise InputDataError(
                f"{where}: 期望 int64，实际得到 {json_type_name(value)}: {value!r}"
            )
        if not (INT64_MIN <= value <= INT64_MAX):
            raise InputDataError(f"{where}: int64 越界: {value}")
        return value
    if declared == ScalarType.TEXT:
        if not isinstance(value, str):
            raise InputDataError(
                f"{where}: 期望 text，实际得到 {json_type_name(value)}: {value!r}"
            )
        return value
    if declared == ScalarType.BOOL:
        if not isinstance(value, bool):
            raise InputDataError(
                f"{where}: 期望 bool，实际得到 {json_type_name(value)}: {value!r}"
            )
        return value
    raise AssertionError(declared)
