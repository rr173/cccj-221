"""AST 校验与错误拒绝测试。"""

import unittest

from bitgroup.astutil import validate_ast
from bitgroup.errors import QueryError

PS = {"region": "text", "level": "int64", "flag": "bool"}
CS = {"temp": "int64", "source": "text"}


def cmp(domain, attr, kind="eq", value=1):
    return {"op": "cmp", "domain": domain, "attr": attr,
            "op_kind": kind, "value": value}


class AstValidationCase(unittest.TestCase):
    def check_bad(self, node):
        with self.assertRaises(QueryError):
            validate_ast(node, PS, CS)

    def test_unknown_attribute(self):
        self.check_bad(cmp("parent", "nope"))
        self.check_bad(cmp("child", "nope"))

    def test_bad_domain_and_op_kind(self):
        self.check_bad({"op": "cmp", "domain": "sideways",
                        "attr": "level", "op_kind": "eq", "value": 1})
        self.check_bad(cmp("parent", "level", kind="like"))

    def test_type_mismatch_literals(self):
        # bool 列不能与数字比较
        self.check_bad(cmp("parent", "flag", value=1))
        # int 列不能与字符串比较
        self.check_bad(cmp("parent", "level", value="1"))
        # text 列不支持大小比较
        self.check_bad(cmp("parent", "region", kind="lt", value="a"))

    def test_count_k_must_be_nonneg_int(self):
        node = {"op": "count_where", "k": -1, "arg": cmp("child", "temp")}
        self.check_bad(node)
        node = {"op": "count_where", "k": True, "arg": cmp("child", "temp")}
        self.check_bad(node)

    def test_child_predicate_cannot_reference_parent(self):
        node = {"op": "exists", "arg": cmp("parent", "region", value="N")}
        self.check_bad(node)

    def test_exists_cannot_nest_in_child(self):
        # exists 的 child_predicate 内再出现 exists，必须拒绝
        node = {"op": "exists", "arg": {
            "op": "and", "args": [
                cmp("child", "temp"),
                {"op": "exists", "arg": cmp("child", "source", value="X")},
            ]}}
        self.check_bad(node)
        # count_where 同理
        node2 = {"op": "count_where", "k": 1, "arg": {
            "op": "or", "args": [
                cmp("child", "temp"),
                {"op": "count_where", "k": 1,
                 "arg": cmp("child", "source", value="X")},
            ]}}
        self.check_bad(node2)

    def test_and_or_need_args(self):
        self.check_bad({"op": "and", "args": []})
        self.check_bad({"op": "or"})

    def test_null_literal_allowed_any_type(self):
        # null 字面量对任意类型合法，校验通过
        for attr in PS:
            validate_ast(cmp("parent", attr, value=None), PS, CS)

    def test_unknown_op(self):
        self.check_bad({"op": "xor", "args": []})


if __name__ == "__main__":
    unittest.main()
