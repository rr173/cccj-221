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
  compare.py    新旧规则历史对照（只读九格对照，输出按日期压缩的变更带）
  review.py     现场复核包导出（三态名额 + 整组子记录预算，原子发布）
  cli.py        命令行入口
scripts/
  generate_scale.py  规模数据生成器（默认 1M 主 / 10M 子）
  bench_scale.py     128MiB 地址空间约束下的基准与观测
  ledger_demo.py     台账小夹具复跑 + 独立核对（区间表与来源登记）
  ledger_contrast.py 历史对照实验（证明更新不扫描旧历史）
  compare_demo.py    新旧规则对照小样本复跑 + 逐日逐对象独立核对
  compare_scale.py   对照规模证据（长稳定历史 + 零散不可比日）
  review_demo.py     复核包小样本独立核对 + 偏斜大组导出面观测
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

## 新旧规则历史对照（只读）

试跑期间，同一批日报索引被两套规则各自记进一份台账（左=老规则，右=新
规则）。上线前的迁移评估要回答：**同一批对象在两套规则下分别落入哪一
格，这些格子的成员是否稳定**（例如“老 false、新 true”的人数连续两天都
是 50，是完全同一批人，还是已经换了一轮）。`ledger-compare` 输入左、右
两份现成的 SQLite 台账和一个闭合自然日窗口，输出一份按日期压缩的
JSONL 变更带：

```bash
python -m bitgroup.cli ledger-compare \
  --left ./old_rule.db --right ./new_rule.db \
  --start 2026-03-01 --end 2026-03-31 --out band.jsonl
# JSONL 变更带写 --out（缺省为标准输出）；处理量指标写标准错误
```

### 每天先判定能不能比较

只看两边 `daily_source` 在**当天**的登记：

| 状态 | 含义 |
| --- | --- |
| `comparable` | 两边都登记了这一天，且 `index_token` 完全相等 |
| `different_source` | 都登记了，但当天代号不同 |
| `left_missing` / `right_missing` | 只有一边登记（指缺登记的一边） |
| `both_missing` | 两边都没有登记 |

只认当天的代号（同一代号出现在别的日期不算），不看人数、不看规则指纹；
空批次的同源日也是可比的（九格全零）。一次命令对每份台账各固定一个
**只读快照**（`mode=ro` + 显式读事务）：来源登记与状态区间永远来自同一
份提交，台账可以边用边继续登记；命令不修改台账，也不需要打开任何日报
索引或原始文件。

### 变更带行格式

每行一个 JSON 对象：`first_day` / `last_day`（`YYYY-MM-DD`，两端都含）、
`status`、`cells`。整段窗口无重无漏、按时间递增；只有 `status` 与整组
`cells` 都相同的相邻自然日才合并，且必须合并。

- 不可比段：`"cells": null`。
- 可比段：固定九格，顺序为左 `true,false,unknown` × 右
  `true,false,unknown`，每格
  `{"left": ..., "right": ..., "count": N, "entered": M}`，
  左右格坐标显式可辨，零人数格也在。
- `count`：当天在该格的人数（只计入两侧当日都有区间的对象，缺席不算
  `unknown`；同一对象当天两边同时变状态，直接按新状态对归格）。
- `entered`：这些人里**前一自然日未在同一格**的人数（包括前一天不在
  对照对象内的人）。若前一日不在窗口内或不可比，则当天九格 `entered`
  全为 `null`——不是零，也不回溯到较早的可比日。

### 处理量与复跑

主要工作量由窗口相关的状态区间记录与日来源记录决定：区间读取是对
`episode_segment` 的一次主键序顺序筛选；每个对象的左/右区间流式归并成
“状态对游程”，只把游程端点事件写磁盘暂存再排序聚合——不把台账还原成
逐对象逐日记录，也不把长区间按可比日段复制相交；零散未对齐日只增加状态
游程与输出行，不放大稳定对象的工作量。每次运行的 `metrics`（JSONL 之外
写标准错误）给出区间行数、游程数、事件数、状态游程数、输出行数与
SQLite VM 步数等可观察量。

```bash
# 小样本复跑 + 逐日逐对象独立枚举核对（含甲乙互换、不同代号日、
# unknown、对象出入、窗口裁剪；同时验证 CLI 与压缩边界）
PYTHONPATH=. python3 scripts/compare_demo.py --work-dir ./compare_run

# 规模证据：长稳定历史夹杂大量零散不可比日，区分“直接处理区间”与
# “按对象展开日期/可比段”的实现；churn 场景在 128 MiB 约束下对照
PYTHONPATH=. python3 scripts/compare_scale.py --work-dir ./compare_scale_run
```

生成的数据不入库（两个目录均已被 .gitignore 覆盖，脚本默认结束后删除）。

## 现场复核包导出（三态名额 + 整组子记录预算）

现场抽检只有有限的明细处理额度。审核员要从一个已建索引里按规则挑一批
主对象，并把**中选对象的全部子记录**带走离线复核。`review-export` 取代
“按对象数翻页再截断”的做法：

- 按现有三态语义给每个主对象归类（名额为零的类照常判定，不因名额为零
  改变判定）；
- 优先级只由 `SHA-256(UTF-8(seed) + 0x00 + UTF-8(entity_id))` 的摘要字节
  升序决定，摘要相同再比 `entity_id` 的 UTF-8 字节——与导入行序、索引
  局部编号、子表顺序都无关；
- 按该顺序考察：**所在类还有名额**且**全部子记录都装得进剩余额度**才
  中选；中选消耗一个本类名额与相应子记录额度，未中选不消耗、也不阻止
  后面的对象中选。无子记录的对象成本为零，额度用完后仍可入选；不在
  类间挪名额，不要求填满，也不求装入数量最多的组合。

```bash
python -m bitgroup.cli review-export --index ./idx_demo --ast fixtures/q_exists.json \
  --seed s2026 --quota-true 2 --quota-false 1 --quota-unknown 1 \
  --max-children 500 --out ./review_pkg
# 成品目录写 --out；可观察指标（含导出阶段读取计数）写标准错误
```

### 交付物（一个目录，三份文件）

- `parents.jsonl`：按中选优先级逐行，`entity_id`、判定 `state`、以及保留
  索引表声明中全部属性的 `attributes`（业务值，不是列字典编号或位图位号）。
- `children.jsonl`：按相同主对象顺序分组，组内按 `child_id` 的 UTF-8 字节
  升序；每行 `entity_id`、`child_id` 与子表 `attributes`。中选对象的所有
  子记录各导出一次，**包括不满足规则的子记录**；落选对象不出现在任一文件。
- `manifest.json`：索引代号、规范化规则指纹、`seed`、名额、子记录上限、
  三态各自的候选数与中选数，以及实际导出的子记录总数。

空包同样产出三份文件（两份 JSONL 为空）。`null`、空字符串、布尔与整数
按业务值原样恢复，互不混淆。

### 发布与失败

单个输出路径只考虑一个发布者：目标路径已存在（文件、目录或符号链接）
一律拒绝，不覆盖。导出先写到目标同级的隐藏暂存目录，三份文件写完并
`fsync` 后才 `rename` 成最终目录；任一步失败都会清理暂存，最终目录不会
留下可被当成成品的半包。

### 处理量与复跑

- 考察阶段按优先级流式归并：每个主对象的优先级键、判定与组大小先写磁盘
  暂存，再按 `(digest, entity_id)` 顺序读取——不把全体候选、中选名单或
  一个大子组整体装进 Python 内存。
- 规则求值本身要读取 posting/子行（这是判定成本）；**导出阶段**只恢复
  中选对象及其子组属性。单组内 `child_id` 排序超过块上限时走分块排序 +
  多路归并。
- 指标（stderr）：`phases.consider` / `phases.export` 的页读取与
  `child_rows_processed`、`export_child_ids_read`、`export_child_attr_reads`、
  `export_largest_group`、`export_group_sort_runs`、`export_peak_kib` 等。

```bash
# 小样本独立核对（纯 Python 从原始行算三态/优先级/整组成本，再比对输出）：
# 装不下的大组被跳过、后续补位、零额度选无子对象、行序变化逐字节不变、
# 规则变化改占名额、发布前失败不留半包。
# 另：现场生成偏斜大组，验证落选大组不在导出阶段被展开。
PYTHONPATH=. python3 scripts/review_demo.py --work-dir ./review_run --big 200000
```

生成的数据与索引按 `.gitignore` 排除，不提交（`review_run/` 已忽略）。

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
