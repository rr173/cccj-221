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
  ledger.py     历史状态台账（SQLite，按自然日维护连续同态区间）
  cli.py        命令行入口
scripts/
  generate_scale.py  规模数据生成器（默认 1M 主 / 10M 子）
  bench_scale.py     128MiB 地址空间约束下的基准与观测
  ledger_demo.py     台账小夹具复跑 + 独立核对（区间表与来源登记）
  ledger_contrast.py 历史对照实验（证明更新不扫描旧历史）
fixtures/            手算示例小夹具（ledger/ 为历史台账逐日夹具）
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

## 历史状态台账（跨日连续段）

原取群命令只回答“某个已建索引里的判定”。质检还要问：**这个对象从哪天
起连续处于待核状态？中间有没有断档？** 台账把每天的日结登记进一个
SQLite 文件，增量维护每个对象在自然日轴上“连续且同态”的区间，下游直接
用 SQL 读取结果，不必自己翻每天的索引。

### 一份台账固定一套规则与声明

首次 `ledger-register` 时把规范化后的**查询 AST**（现有格式）与其指纹、
主/子 schema 一并写入台账。之后每次命令都带 `--ast`：
`--ast` 与台账不一致、或索引的主/子声明与台账不一致，一律拒绝。
日期**只取 `--date YYYY-MM-DD` 参数**，不从导入顺序、目录名或文件时间推断。
历史日报索引之后可以移走：更新用的旧状态与边界都在 SQLite 里。

```bash
# 首次登记（同时创建台账）
python -m bitgroup.cli ledger-register --ledger ./ledger.db \
  --date 2026-03-02 --index ./idx_0302 --ast fixtures/ledger/query.json

# 补录更早的一天（乱序）；替换同一天（更正版）也走同一条命令
python -m bitgroup.cli ledger-register --ledger ./ledger.db \
  --date 2026-03-01 --index ./idx_0301 --ast fixtures/ledger/query.json

# 下游：读取已算好的状态区间（两端日期都包含）
python -m bitgroup.cli ledger-episodes --ledger ./ledger.db --entity-id A
# 下游：按日来源登记（区分“缺报”与“当日已登记但无该对象”）
python -m bitgroup.cli ledger-sources --ledger ./ledger.db
python -m bitgroup.cli ledger-object-days --ledger ./ledger.db --entity-id A
```

### 语义要点

- 每次接收“台账路径 + 业务日期 + 当天只读索引 + 规则文件”，首次创建台账，
  随后按日期补录或替换。
- 同一天再次登记**同一索引代号**是幂等重送；换新代号即使判定全不变，
  也会更新该日的**来源登记**。
- 跨日以**原始 `entity_id`** 相认，绝不沿用索引内局部编号。
- 某日**未登记**、或当日**已登记但没有该对象**，都会切断连续性，且都
  不产出状态行，绝不当成 `false`/`unknown`。
- 每行 `episode(entity_id, first_day, last_day, state)` 覆盖一段连续同态
  自然日，两端闭合；能合并的相邻段立即合并。
- 一次更新是单个 `BEGIN IMMEDIATE` 写事务，失败整体回滚；WAL 下并发
  读者只看到完整旧版或完整新版，不会“新登记配旧区间”。
- 更新只打开**本次送来**的索引；只对当天新增/消失/变态的对象在其日期处
  查找和调整受影响的段，不全表重建、不遍历全部历史日期、不逐日扫描某个
  更正对象的整条时间线。当日提取与比对按对象流式进行，内存随当天变化量
  增长。可观察量见返回 JSON 的 `metrics`（含 SQLite VM 步数）。

### 从空库复跑小夹具 + 独立核对

```bash
# 夹具在 fixtures/ledger/（day01/02/03、day02_corrected），
# 脚本内部用纯 Python 逐日判定原始 JSONL 再按自然日合并，独立核对
# 整张区间表与来源登记——不复用台账的更新逻辑。
PYTHONPATH=. python3 scripts/ledger_demo.py --work-dir ./ledger_run

# 两份可生成历史对照（同日更正、不同长度无关历史，含长同态段），
# 用真实 SQLite 读数给出“更新未扫描旧历史”的证据；不提交生成数据。
PYTHONPATH=. python3 scripts/ledger_contrast.py --work-dir ./contrast_run
```

### 直接 SQL 读（视图列名固定）

```sql
-- 某对象从哪天起连续处于某态
SELECT * FROM episode WHERE entity_id = 'A' ORDER BY first_day;
-- 每日来源登记
SELECT business_date, index_token FROM daily_source ORDER BY business_date;
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
