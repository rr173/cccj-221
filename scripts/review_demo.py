"""复核包导出：小样本复跑 + 独立核对 + 偏斜数据导出面观测。

两部分，均从空目录开始：

1. **小样本对照**（``fixtures/`` 同款手算集，外加若干补充对象）：
   直接读原始 JSONL，用纯 Python 逐对象算三态、按
   ``SHA-256(seed + NUL + entity_id)`` 排序、逐对象累加整组成本，得到
   中选名单；再跑真实 CLI 导出，逐行核对 ``parents.jsonl`` /
   ``children.jsonl`` / ``manifest.json``。覆盖：

   * 装不下的高优先级大组被跳过、后续候选补位；
   * 子记录额度为零时仍选中无子记录对象；
   * 导入行序变化（主/子行序颠倒、局部编号改变）后输出逐字节不变；
   * 规则变化让同一对象改占另一类名额；
   * 发布前写入失败：返回非零、最终目录不存在、无暂存残留。

2. **偏斜数据**（现场生成、不提交）：一个对象带 ``--big`` 条子记录且
   优先级排在最前，额度只允许导出少量子记录。先检查它确实因“整组装不
   下”被跳过，再用可观察量证明导出阶段没有展开它：
   ``export_child_ids_read`` / ``export_child_attr_reads``（导出阶段实际
   恢复的子身份与子属性数）只覆盖中选的小组，与那个大组的规模无关；
   ``export_peak_kib``（tracemalloc 峰值）不随大组增长。同时给出
   ``child_rows_processed``（规则求值阶段的子行数）作为对照——判定确实
   要看全体子行，而导出只看中选组。

用法::

    PYTHONPATH=. python3 scripts/review_demo.py --work-dir ./review_run
    PYTHONPATH=. python3 scripts/review_demo.py --work-dir ./review_run \\
        --big 200000 --big-children-quota 50

工作目录与生成的大数据按 .gitignore 排除，不提交进仓库。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)

from bitgroup.importer import Importer  # noqa: E402
from bitgroup.review import export_review_package  # noqa: E402

PY = sys.executable

PARENT_SCHEMA = {"region": "text", "level": "int64", "flag": "bool",
                 "note": "null"}
CHILD_SCHEMA = {"temp": "int64", "source": "text", "ok": "bool",
                "tag": "null"}

CMP = lambda d, a, k, v: {"op": "cmp", "domain": d, "attr": a,
                          "op_kind": k, "value": v}

AST_X = {
    "op": "and",
    "args": [
        CMP("parent", "region", "eq", "N"),
        {"op": "exists", "arg": {"op": "and", "args": [
            CMP("child", "temp", "lt", 25),
            CMP("child", "source", "eq", "X"),
        ]}},
    ],
}
# 同一批数据换规则：只按主属性 region 判定，P 会从 unknown 变成 false。
AST_S = CMP("parent", "region", "eq", "S")


# --------------------------------------------------------------------------
# 小样本（手算）
# --------------------------------------------------------------------------

PARENTS = [
    {"entity_id": "P", "region": "N", "level": None, "flag": True,
     "note": None},
    {"entity_id": "Q", "region": "N", "level": 2, "flag": False,
     "note": None},
    {"entity_id": "R", "region": "S", "level": 3, "flag": None,
     "note": None},
    {"entity_id": "S", "region": "", "level": 1, "flag": True,
     "note": None},
    {"entity_id": "T", "region": "N", "level": 0, "flag": False,
     "note": None},
    # 补充：BIG 有 5 条子记录（来源 Z，规则不满足）——用来验证整组导出
    # 与“装不下则跳过大组”。
    {"entity_id": "BIG", "region": "N", "level": 9, "flag": True,
     "note": None},
]

CHILDREN = [
    {"child_id": "p1", "entity_id": "P", "temp": 20, "source": "Y",
     "ok": True, "tag": None},
    {"child_id": "p2", "entity_id": "P", "temp": None, "source": "X",
     "ok": False, "tag": None},
    {"child_id": "q1", "entity_id": "Q", "temp": 20, "source": "X",
     "ok": True, "tag": None},
    {"child_id": "q2", "entity_id": "Q", "temp": 21, "source": "X",
     "ok": None, "tag": None},
    {"child_id": "s1", "entity_id": "S", "temp": 30, "source": "Z",
     "ok": False, "tag": None},
    {"child_id": "s2", "entity_id": "S", "temp": 31, "source": "Z",
     "ok": True, "tag": None},
    # T 无子记录
] + [
    {"child_id": f"big{i}", "entity_id": "BIG", "temp": 10, "source": "X",
     "ok": True, "tag": None} for i in range(5)
]


# ---- 独立实现：只读原始行 ------------------------------------------------


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def read_jsonl(path):
    out = []
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if raw:
                out.append(json.loads(raw))
    return out


def reference_states(parents, children, ast):
    """纯 Python 三态参考实现（不接触引擎/索引）。"""
    by_entity = {}
    for ch in children:
        by_entity.setdefault(ch["entity_id"], []).append(ch)

    def cmp(row, node):
        v, lit = row.get(node["attr"]), node["value"]
        if v is None or lit is None:
            return "unknown"
        ok = {"eq": v == lit, "ne": v != lit, "lt": v < lit,
              "le": v <= lit, "gt": v > lit, "ge": v >= lit}[node["op_kind"]]
        return "true" if ok else "false"

    def and3(ss):
        return ("false" if "false" in ss else
                "unknown" if "unknown" in ss else "true")

    def or3(ss):
        return ("true" if "true" in ss else
                "unknown" if "unknown" in ss else "false")

    def not3(s):
        return {"true": "false", "false": "true", "unknown": "unknown"}[s]

    def eval_child(node, row):
        op = node["op"]
        if op == "cmp":
            return cmp(row, node)
        if op == "and":
            return and3([eval_child(a, row) for a in node["args"]])
        if op == "or":
            return or3([eval_child(a, row) for a in node["args"]])
        if op == "not":
            return not3(eval_child(node["arg"], row))
        raise ValueError(op)

    def lift(node, eid):
        sts = [eval_child(node["arg"], r) for r in by_entity.get(eid, [])]
        t, u = sts.count("true"), sts.count("unknown")
        if node["op"] == "exists":
            return "true" if t >= 1 else ("unknown" if t + u >= 1 else "false")
        k = node["k"]
        if t >= k:
            return "true"
        if t + u < k:
            return "false"
        return "unknown"

    def eval_parent(node, prow):
        op = node["op"]
        if op == "cmp":
            return cmp(prow, node)
        if op == "and":
            return and3([eval_parent(a, prow) for a in node["args"]])
        if op == "or":
            return or3([eval_parent(a, prow) for a in node["args"]])
        if op == "not":
            return not3(eval_parent(node["arg"], prow))
        return lift(node, prow["entity_id"])

    return {p["entity_id"]: eval_parent(ast, p) for p in parents}


def reference_plan(parents, children, states, seed, quotas, max_children):
    """独立算中选名单：纯排序 + 贪心累加整组成本。"""
    order = sorted(parents, key=lambda p: (
        hashlib.sha256(seed.encode("utf-8") + b"\x00" +
                       p["entity_id"].encode("utf-8")).digest(),
        p["entity_id"].encode("utf-8")))
    cost = {}
    kept = {}
    for ch in children:
        cost[ch["entity_id"]] = cost.get(ch["entity_id"], 0) + 1
        kept.setdefault(ch["entity_id"], []).append(ch)
    remaining = dict(quotas)
    rem_children = max_children
    selected = []
    for p in order:
        eid = p["entity_id"]
        if remaining[states[eid]] <= 0:
            continue
        if cost.get(eid, 0) > rem_children:
            continue
        remaining[states[eid]] -= 1
        rem_children -= cost.get(eid, 0)
        selected.append(eid)
    return selected, cost, kept


def prefix(seed, eid, n=10):
    return hashlib.sha256(seed.encode("utf-8") + b"\x00" +
                          eid.encode("utf-8")).hexdigest()[:n]


def find_seed(predicate, limit=5000):
    for i in range(limit):
        s = f"seed{i}"
        if predicate(s):
            return s
    raise AssertionError("找不到满足条件的 seed")


# ---- 校验 ----------------------------------------------------------------


def check_package(pkg, selected, states, cost, kept, parents, children, ast):
    got_p = read_jsonl(os.path.join(pkg, "parents.jsonl"))
    got_c = read_jsonl(os.path.join(pkg, "children.jsonl"))
    problems = []
    if [r["entity_id"] for r in got_p] != selected:
        problems.append("parents 顺序与独立中选名单不一致")
    by_id = {p["entity_id"]: p for p in parents}
    for row in got_p:
        eid = row["entity_id"]
        if row["state"] != states[eid]:
            problems.append(f"state 不符 {eid}")
        want_attrs = {k: by_id[eid][k] for k in PARENT_SCHEMA}
        if row["attributes"] != want_attrs:
            problems.append(f"主属性不符 {eid}")
    # 子行：按主对象顺序分组，组内 child_id UTF-8 字节序，整组不多不少
    groups = {}
    order = []
    for row in got_c:
        eid = row["entity_id"]
        if eid not in groups:
            groups[eid] = []
            order.append(eid)
        groups[eid].append(row)
    if order != [e for e in selected if kept.get(e)]:
        problems.append("children 组顺序与 parents 不一致")
    for eid, rows in groups.items():
        want_ids = sorted([c["child_id"] for c in kept.get(eid, [])],
                          key=lambda s: s.encode("utf-8"))
        got_ids = [r["child_id"] for r in rows]
        if got_ids != want_ids:
            problems.append(f"子行集合/顺序不符 {eid}")
        src = {c["child_id"]: c for c in kept.get(eid, [])}
        for row in rows:
            want = {k: src[row["child_id"]][k] for k in CHILD_SCHEMA}
            if row["attributes"] != want:
                problems.append(f"子属性不符 {eid}/{row['child_id']}")
    manifest = json.load(open(os.path.join(pkg, "manifest.json"),
                              encoding="utf-8"))
    if manifest["n_parents"] != len(selected):
        problems.append("manifest n_parents 不符")
    if manifest["n_children"] != sum(cost.get(e, 0) for e in selected):
        problems.append("manifest n_children 不符")
    return problems, got_p, got_c, manifest


def compare_dirs(a, b, names):
    diffs = []
    for name in names:
        pa, pb = os.path.join(a, name), os.path.join(b, name)
        with open(pa, "rb") as fa, open(pb, "rb") as fb:
            if fa.read() != fb.read():
                diffs.append(name)
    return diffs


def build_index(work, tag, parents, children, schema=None):
    base = os.path.join(work, "indexes", tag)
    os.makedirs(base)
    pp, cp = os.path.join(base, "parent.jsonl"), os.path.join(base, "child.jsonl")
    write_jsonl(pp, parents)
    write_jsonl(cp, children)
    idx = os.path.join(base, "index")
    Importer(idx, schema[0], schema[1]).run(pp, cp)
    return idx, pp, cp


def run_cli(args):
    return subprocess.run([PY, "-m", "bitgroup.cli"] + args,
                          capture_output=True, text=True, cwd=HERE)


# --------------------------------------------------------------------------
# 小样本复跑
# --------------------------------------------------------------------------


def small_sample(work):
    report = {"checks": {}, "problems": []}
    schema = (PARENT_SCHEMA, CHILD_SCHEMA)
    idx, pp, cp = build_index(work, "demo", PARENTS, CHILDREN, schema)
    states = reference_states(PARENTS, CHILDREN, AST_X)
    report["states_AST_X"] = states

    # --- 1. 全额度：中选名单 = 完整优先级序 -----------------------------
    quotas = {"true": 5, "false": 5, "unknown": 5}
    sel, cost, kept = reference_plan(PARENTS, CHILDREN, states, "seeddemo",
                                     quotas, 1000)
    pkg = os.path.join(work, "pkg_all")
    res = export_review_package(idx, AST_X, seed="seeddemo",
                                quota_true=5, quota_false=5,
                                quota_unknown=5, max_children=1000,
                                out_dir=pkg)
    problems, got_p, got_c, manifest = check_package(
        pkg, sel, states, cost, kept, PARENTS, CHILDREN, AST_X)
    report["checks"]["full_quota_matches_independent"] = not problems
    report["problems"].extend(problems)
    report["full_quota_order"] = [r["entity_id"] for r in got_p]
    report["manifest_all"] = {
        "candidates": manifest["candidates"],
        "selected": manifest["selected"],
        "n_children": manifest["n_children"],
    }

    # --- 2. 装不下的大组被跳过、后续小组补位 ---------------------------
    # 选一个让 BIG 排在最前的 seed，额度只够 2 条子记录。
    seed_big = find_seed(lambda s: sorted(
        [p["entity_id"] for p in PARENTS],
        key=lambda e: (hashlib.sha256(
            s.encode() + b"\x00" + e.encode()).digest(), e.encode()))[0] == "BIG")
    q2 = {"true": 3, "false": 0, "unknown": 0}
    sel2, cost2, kept2 = reference_plan(PARENTS, CHILDREN, states, seed_big,
                                        q2, 2)
    assert "BIG" not in sel2, "BIG 成本 5 > 额度 2，必须跳过"
    pkg2 = os.path.join(work, "pkg_skip")
    res2 = export_review_package(idx, AST_X, seed=seed_big, quota_true=3,
                                 quota_false=0, quota_unknown=0,
                                 max_children=2, out_dir=pkg2)
    problems2, _, _, _ = check_package(pkg2, sel2, states, cost2, kept2,
                                       PARENTS, CHILDREN, AST_X)
    report["checks"]["big_group_skipped_backfilled"] = (
        not problems2 and "BIG" not in [r["entity_id"]
                                        for r in read_jsonl(
                                            os.path.join(pkg2, "parents.jsonl"))]
        and res2.manifest["selected"]["true"] == len(sel2))
    report["problems"].extend(problems2)
    report["skip_case"] = {
        "seed": seed_big, "selected": sel2, "cost_BIG": cost["BIG"],
        "child_budget_skips": res2.metrics["child_budget_skips"],
    }

    # --- 3. 额度为零仍选无子记录对象 -----------------------------------
    q3 = {"true": 5, "false": 5, "unknown": 5}
    sel3, cost3, kept3 = reference_plan(PARENTS, CHILDREN, states, "zero",
                                        q3, 0)
    pkg3 = os.path.join(work, "pkg_zero")
    res3 = export_review_package(idx, AST_X, seed="zero", quota_true=5,
                                 quota_false=5, quota_unknown=5,
                                 max_children=0, out_dir=pkg3)
    problems3, _, _, _ = check_package(pkg3, sel3, states, cost3, kept3,
                                       PARENTS, CHILDREN, AST_X)
    report["checks"]["zero_budget_childless_selected"] = (
        not problems3 and "T" in sel3 and
        all(cost.get(e, 0) == 0 for e in sel3))
    report["problems"].extend(problems3)
    report["zero_budget_selected"] = sel3

    # --- 4. 导入顺序变化：输出逐字节不变 -------------------------------
    idx_rev, _, _ = build_index(work, "demo_rev", list(reversed(PARENTS)),
                                list(reversed(CHILDREN)), schema)
    pkgA = os.path.join(work, "pkg_ord_a")
    pkgB = os.path.join(work, "pkg_ord_b")
    export_review_package(idx, AST_X, seed="ord", quota_true=1,
                          quota_false=1, quota_unknown=1,
                          max_children=3, out_dir=pkgA)
    export_review_package(idx_rev, AST_X, seed="ord", quota_true=1,
                          quota_false=1, quota_unknown=1,
                          max_children=3, out_dir=pkgB)
    diffs = compare_dirs(pkgA, pkgB, ("parents.jsonl", "children.jsonl"))
    report["checks"]["row_order_invariance_bytes"] = not diffs
    if diffs:
        report["problems"].append(f"行序变化改变了输出: {diffs}")

    # --- 5. 规则变化：同一对象改占另一类名额 ---------------------------
    states_S = reference_states(PARENTS, CHILDREN, AST_S)
    q_unknown = {"true": 0, "false": 0, "unknown": 2}
    sel_u, cost_u, kept_u = reference_plan(PARENTS, CHILDREN, states_S,
                                           "rule", q_unknown, 1000)
    pkg_u = os.path.join(work, "pkg_rule_u")
    export_review_package(idx, AST_S, seed="rule", quota_true=0,
                          quota_false=0, quota_unknown=2, max_children=1000,
                          out_dir=pkg_u)
    problems_u, got_u, _, man_u = check_package(
        pkg_u, sel_u, states_S, cost_u, kept_u, PARENTS, CHILDREN, AST_S)
    q_false = {"true": 0, "false": 1, "unknown": 0}
    sel_f, cost_f, kept_f = reference_plan(PARENTS, CHILDREN, states_S,
                                           "rule", q_false, 1000)
    pkg_f = os.path.join(work, "pkg_rule_f")
    export_review_package(idx, AST_S, seed="rule", quota_true=0,
                          quota_false=1, quota_unknown=0, max_children=1000,
                          out_dir=pkg_f)
    problems_f, got_f, _, man_f = check_package(
        pkg_f, sel_f, states_S, cost_f, kept_f, PARENTS, CHILDREN, AST_S)
    # 老规则下 P 是 unknown：false 名额选不到它（unknown 名额为 0）。
    q_false_x = {"true": 0, "false": 1, "unknown": 0}
    sel_fx, cost_fx, kept_fx = reference_plan(PARENTS, CHILDREN, states,
                                              "rule", q_false_x, 1000)
    pkg_fx = os.path.join(work, "pkg_rule_fx")
    export_review_package(idx, AST_X, seed="rule", quota_true=0,
                          quota_false=1, quota_unknown=0, max_children=1000,
                          out_dir=pkg_fx)
    problems_fx, got_fx, _, man_fx = check_package(
        pkg_fx, sel_fx, states, cost_fx, kept_fx, PARENTS, CHILDREN, AST_X)
    p_false_after = [r["entity_id"] for r in got_f]
    p_false_before = [r["entity_id"] for r in got_fx]
    report["checks"]["rule_change_moves_class_quota"] = (
        not problems_u and not problems_f and not problems_fx
        and states["P"] == "unknown" and states_S["P"] == "false"
        and "P" in p_false_after            # 新规则：P 占用 false 名额
        and "P" not in p_false_before       # 老规则：P 不在 false 名额里
        and man_fx["selected"]["unknown"] == 0)
    report["problems"].extend(problems_u + problems_f + problems_fx)
    report["rule_change"] = {
        "P_state_AST_X": states["P"], "P_state_AST_S": states_S["P"],
        "unknown_quota_selected_AST_S": [r["entity_id"] for r in got_u],
        "false_quota_selected_AST_X": p_false_before,
        "false_quota_selected_AST_S": p_false_after,
    }

    # --- 6. 发布前写入失败：CLI 返回非零，无成品/半包 -----------------
    fail_out = os.path.join(work, "pkg_fail")

    def boom():
        raise OSError("simulated write failure before publish")

    try:
        export_review_package(idx, AST_X, seed="x", quota_true=1,
                              quota_false=1, quota_unknown=1,
                              max_children=1000, out_dir=fail_out,
                              before_publish=boom)
        failed = False
    except OSError:
        failed = True
    leftovers = [n for n in os.listdir(work)
                 if n.startswith(".bitgroup_review_stage_")]
    report["checks"]["publish_failure_no_partial"] = (
        failed and not os.path.lexists(fail_out) and not leftovers)
    if not report["checks"]["publish_failure_no_partial"]:
        report["problems"].append("发布失败留下了成品或暂存")

    # --- 7. 输出路径已存在拒绝覆盖（CLI 返回非零） ---------------------
    existing = os.path.join(work, "pkg_existing")
    os.makedirs(existing)
    marker = os.path.join(existing, "keep.txt")
    open(marker, "w").write("keep")
    proc = run_cli([
        "review-export", "--index", idx, "--ast",
        os.path.join(work, "rule.json"), "--seed", "x",
        "--quota-true", "1", "--quota-false", "1", "--quota-unknown", "1",
        "--max-children", "10", "--out", existing])
    report["checks"]["existing_path_cli_nonzero"] = (
        proc.returncode != 0 and os.listdir(existing) == ["keep.txt"])
    if proc.returncode == 0:
        report["problems"].append("已有路径未被 CLI 拒绝")

    # --- 8. 空包三份文件 ------------------------------------------------
    empty = os.path.join(work, "pkg_empty")
    export_review_package(idx, AST_X, seed="x", quota_true=0,
                          quota_false=0, quota_unknown=0, max_children=0,
                          out_dir=empty)
    names = sorted(os.listdir(empty))
    report["checks"]["empty_package_three_files"] = (
        names == ["children.jsonl", "manifest.json", "parents.jsonl"]
        and read_jsonl(os.path.join(empty, "parents.jsonl")) == []
        and read_jsonl(os.path.join(empty, "children.jsonl")) == [])
    report["empty_manifest_candidates"] = json.load(
        open(os.path.join(empty, "manifest.json")))["candidates"]

    return report


# --------------------------------------------------------------------------
# 偏斜数据：落选大组不在导出阶段被展开
# --------------------------------------------------------------------------


def skewed(work, big, small_target=3):
    """偏斜数据：BIG 巨额子记录且优先级最高、额度装不下 → 落选。

    第二轮让 BIG「装得下」但名额只够它一个、再加一个只有 1 条子记录的小
    对象，验证单组内超过块上限时的分块排序仍只按整组预算计费。
    """
    report = {"big_group_size": big}
    rows_p = [{"entity_id": "BIG", "region": "N", "level": 1,
               "flag": True, "note": None}]
    rows_c = [{"child_id": f"big{i:08d}", "entity_id": "BIG",
               "temp": 10, "source": "X", "ok": True, "tag": None}
              for i in range(big)]
    for j in range(6):
        eid = f"S{j}"
        rows_p.append({"entity_id": eid, "region": "N", "level": j + 2,
                       "flag": False, "note": None})
        rows_c.append({"child_id": f"{eid}-0", "entity_id": eid,
                       "temp": 10, "source": "X", "ok": True, "tag": None})
    idx, _, _ = build_index(work, "skew", rows_p, rows_c,
                            (PARENT_SCHEMA, CHILD_SCHEMA))
    # 让 BIG 的优先级排在最前，且额度只能导出 3 条子记录
    seed = find_seed(lambda s: sorted(
        [p["entity_id"] for p in rows_p],
        key=lambda e: (hashlib.sha256(
            s.encode() + b"\x00" + e.encode()).digest(), e.encode()))[0] == "BIG")
    out = os.path.join(work, "pkg_skew")
    res = export_review_package(idx, AST_X, seed=seed, quota_true=1,
                                quota_false=0, quota_unknown=0,
                                max_children=small_target, out_dir=out)
    parents = read_jsonl(os.path.join(out, "parents.jsonl"))
    children = read_jsonl(os.path.join(out, "children.jsonl"))
    m = res.metrics
    report.update({
        "seed": seed,
        "selected": [r["entity_id"] for r in parents],
        "exported_children": len(children),
        "big_group_selected": any(r["entity_id"] == "BIG" for r in parents),
        "child_budget_skips": m["child_budget_skips"],
        # 规则求值阶段确实看了全体子行（判定成本）
        "consider_child_rows_processed":
            m["phases"]["consider"]["child_rows_processed"],
        # 导出阶段只恢复中选组
        "export_child_ids_read": m["export_child_ids_read"],
        "export_child_attr_reads": m["export_child_attr_reads"],
        "export_groups": m["export_groups"],
        "export_peak_kib": m["export_peak_kib"],
        "export_group_sort_runs": m["export_group_sort_runs"],
        "export_largest_group": m["export_largest_group"],
    })
    # 证明：落选大组没有被展开——导出读的子身份/属性数 ≤ 实际导出的子行数
    per_child = len(CHILD_SCHEMA)
    report["checks"] = {
        "big_group_not_selected": not report["big_group_selected"],
        "export_reads_only_selected":
            m["export_child_ids_read"] <= len(children)
            and m["export_child_attr_reads"] <= len(children) * per_child,
        "consider_saw_all_children":
            m["phases"]["consider"]["child_rows_processed"] >= big,
        "export_peak_independent_of_big":
            (m["export_peak_kib"] is not None
             and m["export_peak_kib"] < 8 * 1024),
    }

    # ---- 第二轮：BIG 中选，单组内分块排序（内存在块上限） -------------
    out2 = os.path.join(work, "pkg_skew_big")
    res2 = export_review_package(
        idx, AST_X, seed=seed, quota_true=1, quota_false=0,
        quota_unknown=0, max_children=big, out_dir=out2)
    parents2 = read_jsonl(os.path.join(out2, "parents.jsonl"))
    children2 = read_jsonl(os.path.join(out2, "children.jsonl"))
    m2 = res2.metrics
    report["big_selected_case"] = {
        "selected": [r["entity_id"] for r in parents2],
        "exported_children": len(children2),
        "export_group_sort_runs": m2["export_group_sort_runs"],
        "export_group_chunk_max_entries":
            m2["export_group_chunk_max_entries"],
        "export_peak_kib": m2["export_peak_kib"],
    }
    report["checks"].update({
        "big_selected_exports_whole_group":
            [r["entity_id"] for r in parents2] == ["BIG"]
            and len(children2) == big
            and [c["child_id"] for c in children2]
            == sorted([c["child_id"] for c in children2],
                      key=lambda x: x.encode("utf-8")),
        "big_group_sorted_in_chunks":
            m2["export_group_sort_runs"] > 0
            and m2["export_group_chunk_max_entries"] <= 8192,
        "big_group_export_memory_bounded":
            m2["export_peak_kib"] is not None
            and m2["export_peak_kib"] < 8 * 1024,
    })
    return report


# --------------------------------------------------------------------------


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", required=True)
    ap.add_argument("--big", type=int, default=200000,
                    help="偏斜数据里大组的子记录数（现场生成，不提交）")
    ap.add_argument("--keep", action="store_true",
                    help="保留生成的数据与索引（默认删除）")
    args = ap.parse_args(argv)
    work = os.path.abspath(args.work_dir)
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)

    schema = {"parent": PARENT_SCHEMA, "child": CHILD_SCHEMA}
    with open(os.path.join(work, "schema.json"), "w", encoding="utf-8") as fh:
        json.dump(schema, fh, ensure_ascii=False, indent=2)
    with open(os.path.join(work, "rule.json"), "w", encoding="utf-8") as fh:
        json.dump(AST_X, fh, ensure_ascii=False, indent=2)

    small = small_sample(work)
    skew = skewed(work, args.big)

    ok = (not small["problems"]
          and all(small["checks"].values())
          and all(skew["checks"].values()))
    report = {"ok": ok, "small_sample": small, "skewed": skew}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not args.keep:
        # 只删除生成的大数据/索引，保留小样本包便于人工比对
        for name in ("indexes/skew", "pkg_skew", "pkg_skew_big"):
            shutil.rmtree(os.path.join(work, name), ignore_errors=True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
