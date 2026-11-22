# bitgroup — 可重开的磁盘位图取群引擎

面向“主记录 + 多条子记录”的三态（true / false / unknown）集合查询引擎。
导入主表与子表 JSONL 后生成**只读索引代号**，之后可反复执行带缺失值语义的
集合条件，给出三类计数、可分页对象 ID、分面统计与逐节点解释。原 JSONL
移走后仍可查询。

纯 Python 标准库实现，不依赖任何第三方包；Python ≥ 3.11。

## 它解决的问题

质检场景常按“属于 N 区，且**至少两条检测同时满足**温度合格且来源为 X”
筛选对象。关键约束：**不能把一条检测的温度与另一条的来源拼起来**。
引擎先在子行层把谓词算成三态，再按 `child_range` 提升到父对象：

- `exists(p)`：任一子行 true → true；无 true 但有 unknown → unknown；
  否则 false。
- `count_where(p) >= k`：true 数为下界、true+unknown 数为上界；
  下界 ≥ k 为 true，上界 < k 为 false，其余 unknown。
- `not` 的补集以**当前索引完整主对象域**为准（无子行对象同样参与取反）。
- 比较任一边为 null → unknown；布尔运算遵守 Kleene 三值逻辑。

## 目录结构

```
bitgroup/
  types.py      标量类型与校验（int64/text/bool/null；bool≠1；""有值）
  errors.py     统一异常
  iomon.py      可观察 I/O 与临时位图峰值计数器
  pages.py      落盘分页位图：RLE 压缩页、pread 按需加载、LRU、临时 spool
  ident.py      主/子身份 ⇄ 局部编号可逆映射（磁盘哈希目录、blob、range 表）
  column.py     列存点查 + 倒排 posting 物化（含外部递归 MSD 基数排序）
  postings.py   比较 → 按需打开的取值位图（稀疏条件不读无关属性文件）
  importer.py   JSONL 两阶段导入（外部哈希分区、流式身份、悬空/重复校验）
  astutil.py    AST 校验、指纹、HMAC 分页令牌
  engine.py     三态集合执行、提升、分页、分面、explain-entity
  reference.py  独立逐行参考解释器（不接触产品位图/提升逻辑，仅用于核对）
  cli.py        命令行入口
scripts/
  generate_scale.py  规模数据生成器（默认 1M 主 / 10M 子）
  bench_scale.py     128MiB 地址空间约束下的基准与观测
fixtures/            手算示例小夹具
tests/               unittest 套件（含与参考解释器的全量交叉核对）
```

## 从空目录复跑

```bash
# 1) 手算小夹具（P/Q/R，见 fixtures/）
python -m bitgroup.cli build --index ./idx_demo \
  --parent-schema '{"region":"text","level":"int64"}' \
  --child-schema  '{"temp":"int64","source":"text","ok":"bool"}' \
  --parent fixtures/parent.jsonl --child fixtures/child.jsonl

# 2) 三态计数 + 分页
python -m bitgroup.cli query --index ./idx_demo --ast fixtures/q_exists.json
python -m bitgroup.cli query --index ./idx_demo --ast fixtures/q_count2.json

# 3) 分面与逐节点解释
python -m bitgroup.cli facets  --index ./idx_demo --ast fixtures/q_exists.json --attr region
python -m bitgroup.cli explain --index ./idx_demo --ast fixtures/q_exists.json --entity-id P

# 4) 单元测试（标准库 unittest，无需 pytest）
PYTHONPATH=. python -m unittest discover -s tests -v
```

## 规模化（128 MiB 约束，数据与索引不入库）

```bash
# 生成 + 建索引 + 稀疏/稠密/嵌套查询，全程 RLIMIT_AS=128MiB
PYTHONPATH=. python scripts/bench_scale.py \
  --work-dir ./scale_run --parents 1000000 --children 10000000 \
  --memory-mib 128 --cache-mib 16
# 报告写入 ./scale_run/bench_report.json；--skip-generate 可复用已生成数据
```

## 关键设计与可观察性

- **分页位图按需 pread**：65536 位/页，EMPTY/RAW/RLE 三种形态，LRU 页缓存
  预算显式可调；稀疏条件只打开条件涉及的取值位图文件。
- **两个编号域**：父 pid 与子 cid 各自独立，身份映射通过磁盘哈希目录 +
  长度前缀 blob 双向可逆；父子归属由定长 `child_range` 表（mmap）决定，
  不靠实体顺序猜测。
- **临时位图峰值可观测**：中间三态集合是 `TempBitmap`，脏页被 LRU 驱逐时
  顺序追加到 tmp spool；`IOMonitor` 记录 `temp_live_logical_peak`、
  `temp_resident_peak`、`page_reads/bytes_read`、`child_rows_processed`、
  `posting_files_opened` 等真实指标。
- **令牌绑定**：分页令牌含索引代号、AST 指纹、状态与偏移的 HMAC，
  换查询/换索引/换状态/换页大小一律拒绝。

## 输入错误即拒绝

悬空子表 `entity_id`、重复 `entity_id`/`child_id`、类型不符、bool 传数字、
int64 越界、缺属性（null 需显式写 null）都会在导入期抛 `InputDataError`。
