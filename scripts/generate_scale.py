"""大规模 JSONL 数据生成器（流式、低内存）。

生成规模可配置，默认 1,000,000 主对象 / 10,000,000 子行。属性设计兼顾
稀疏与稠密条件：

* 主表 region 为低基数分类（N/S/E/W…），level 为宽域 int64，flag bool，
  note 为全 null 列；
* 子表 temp 宽域 int64（约 5% null），source 低基数 text（含稀疏值 Z），
  ok bool（约 10% null），tag 全 null；
* 每个主对象的子行数用确定性伪随机分布，制造部分“极大子组”。

用法::

    python scripts/generate_scale.py --out-dir /data/scale \\
        --parents 1000000 --children 10000000
"""

from __future__ import annotations

import argparse
import json
import os
import random

REGIONS = ["N", "S", "E", "W", "C"]
SOURCES = ["X", "X", "X", "Y", "Y", "Z"]  # Z 稀疏


def _rng(seed: int) -> random.Random:
    return random.Random(seed)


def generate(out_dir: str, n_parents: int, n_children: int, seed: int = 20261122):
    os.makedirs(out_dir, exist_ok=True)
    parent_path = os.path.join(out_dir, "parent.jsonl")
    child_path = os.path.join(out_dir, "child.jsonl")
    rng = _rng(seed)

    # 先确定每个父对象分到多少子行（含少量极大子组），总和恰为 n_children
    weights = [rng.random() + 0.05 for _ in range(n_parents)]
    huge = set(rng.sample(range(n_parents), max(1, n_parents // 10000)))
    for h in huge:
        weights[h] *= 50
    total_w = sum(weights)
    counts = [max(0, int(n_children * w / total_w)) for w in weights]
    diff = n_children - sum(counts)
    i = 0
    while diff > 0:
        counts[i % n_parents] += 1
        diff -= 1
        i += 1

    with open(parent_path, "w", encoding="utf-8") as pf:
        for pid in range(n_parents):
            region = REGIONS[rng.randrange(len(REGIONS))]
            level = None if rng.random() < 0.02 else rng.randint(0, 1_000_000)
            flag = None if rng.random() < 0.05 else (rng.random() < 0.7)
            pf.write(json.dumps({
                "entity_id": f"e{pid:09d}",
                "region": region,
                "level": level,
                "flag": flag,
                "note": None,
            }, ensure_ascii=False) + "\n")

    cid = 0
    with open(child_path, "w", encoding="utf-8") as cf:
        for pid in range(n_parents):
            for _ in range(counts[pid]):
                temp = None if rng.random() < 0.05 else rng.randint(-20, 60)
                source = SOURCES[rng.randrange(len(SOURCES))]
                ok = None if rng.random() < 0.1 else (rng.random() < 0.8)
                cf.write(json.dumps({
                    "child_id": f"c{cid:010d}",
                    "entity_id": f"e{pid:09d}",
                    "temp": temp,
                    "source": source,
                    "ok": ok,
                    "tag": None,
                }, ensure_ascii=False) + "\n")
                cid += 1
    assert cid == n_children
    return parent_path, child_path


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--parents", type=int, default=1_000_000)
    ap.add_argument("--children", type=int, default=10_000_000)
    ap.add_argument("--seed", type=int, default=20261122)
    args = ap.parse_args(argv)
    p, c = generate(args.out_dir, args.parents, args.children, args.seed)
    print(json.dumps({"parent": p, "child": c,
                      "parents": args.parents, "children": args.children},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
