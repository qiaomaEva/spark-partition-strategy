# 自定义分区实验说明（Custom Partitioner, 同学 C）

本文档说明如何在本项目中复现 **自定义 Partitioner（Custom）实验** 的完整流程，包括：

- 自定义分区策略的大致思想（针对倾斜优化，同时兼顾均匀数据）；  
- 如何调用 `run_custom_basic.sh` 运行实验；
- Spark 如何将事件日志写入 `logs/spark-events`；
- 如何使用 `code/tools/collect_latest_eventlog.py` / `parse_spark_eventlog.py` 解析日志并生成 `code/results/custom_*.json` 结果文件。

> 适用对象：主要面向在 Master 节点上跑实验的同学 A / D，以及需要复现实验的同学。  
> 省流：直接看 **6. 推荐的实验工作流（Custom 分区）**。

---

## 1. 实验脚本与相关文件

Custom 实验主要涉及以下文件：

- 实验作业代码（PySpark）  
  - `code/jobs/custom_basic.py`
- 运行脚本（Shell）  
  - `code/scripts/run_custom_basic.sh`
- Spark 事件日志目录  
  - `logs/spark-events/`
- 事件日志解析工具（Python）  
  - `code/tools/parse_spark_eventlog.py`
  - `code/tools/collect_latest_eventlog.py`
- 解析后结果输出目录  
  - `code/results/`

目录结构与 Range 基本一致，参见 `docs/range_experiment.md` 中的说明。

---

## 2. custom_basic.py：核心逻辑与指标

`code/jobs/custom_basic.py` 实现了一个**针对倾斜数据优化，同时兼顾均匀数据**的自定义分区实验，主要特点：

### 2.1 自定义策略思想（skew-aware）

- 对于热点 key 集合 `hot_keys`：
  - 将同一个热点 key 的记录拆分到多个“子桶”（sub-buckets）；
  - 再通过 `partitionBy(numPartitions, custom_hash)` 将这些子桶映射到多个物理分区，从而缓解单分区的热点；
- 对于非热点 key：
  - 使用普通 `hash(orig_key) % numPartitions`；
  - 在均匀数据 + `hot_keys` 为空的情况下，整体行为退化为普通 hash 分区，不会明显退化性能。

换句话说：

- **倾斜场景**：通过为热点 key 拆桶，使其负载更均匀；  
- **均匀场景**：如果不指定热点 key，自定义分区器和标准 hash 分区几乎等价。

### 2.2 输入类型与参数

Custom 与 Range 一样，使用 Spark **内部生成**的数据，不再依赖外部 CSV：

- `--input-type {synthetic, uniform, skewed}`：三种数据生成模式（全部在 Spark 内部生成）
  - `synthetic`：简单的 `(key, value)` 测试数据；
  - `uniform`：在 Spark 内部生成均匀分布的 `(key, value)`；
  - `skewed`：在 Spark 内部生成带热点 key 的倾斜数据；
- 主要参数：
  - `--num-records`：生成记录数；
  - `--num-partitions`：逻辑分区数（自定义分区前后都保持一致）；
  - `--hot-keys`：逗号分隔的热点 key 列表，例如 `"0"` 或 `"0,1,2"`；
    - 若在 `--input-type=skewed` 场景下不指定（或传空字符串），程序会自动将数据生成时使用的 `HOT_KEY` 视为唯一热点 key；
  - `--hot-bucket-factor`：每个热点 key 拆成多少个子桶位（例如 4、8、32）。**该值不会被程序自动修改，实际使用的桶数与命令行保持一致**；
  - `--print-sample`：是否打印结果样本，仅本地调试使用。

### 2.3 输出指标（stdout）

`custom_basic.py` 在 stdout 中会输出以下指标：

- `t_partition_seconds`：`partitionBy` 阶段耗时；
- `t_agg_seconds`：`reduceByKey` 阶段耗时；
- `t_total_seconds_approx`：两者之和（近似）；
- `partition_distribution_before`：原始 RDD 各分区记录数；
- `partition_distribution_after_custom`：自定义分区后各分区记录数；
- `partition_distribution_after_reduce_raw`：按 `(key, sub_bucket)` 聚合后，各分区记录数；
- `partition_distribution_final`：按**原始 key** 再次汇总后的最终分区分布（最新代码已添加）。

运行时，程序会首先打印本次实验的关键参数，例如：

```text
===== Custom Partitioner Basic Experiment (by C) =====
Input type        : uniform
Num records       : 200000
Num partitions    : 8
Hot keys          : 
Hot bucket factor : 4
Print sample      : True
```

同样，`custom_basic.py` 也通过 `SparkConf` 开启了事件日志，将 Job/Stage/Task 级别指标写入项目根目录下的 `logs/spark-events/`。

---

## 3. 使用 run_custom_basic.sh 运行实验

### 3.1 脚本位置与作用

- 路径：`code/scripts/run_custom_basic.sh`  
- 作用：在本地 / Master 上快速运行 `custom_basic.py`，并**自动导出 JSON 结果**到 `code/results/`，文件前缀为 `custom_...`。

### 3.2 本地调试示例（local 模式）

在 WSL / 本地 Linux 环境中，可以先将脚本里的 `--master` 改为 `local[*]`，然后小规模测试：

```bash
cd ~/spark-partition-strategy

# 均匀数据 + 无热点（退化为 hash 分区）
code/scripts/run_custom_basic.sh \
  --input-type uniform \
  --num-records 200000 \
  --num-partitions 8 \
  --hot-keys "" \
  --hot-bucket-factor 4 \
  --print-sample

# 倾斜数据 + 指定热点 key=0
code/scripts/run_custom_basic.sh \
  --input-type skewed \
  --num-records 200000 \
  --num-partitions 8 \
  --hot-keys "0" \
  --hot-bucket-factor 4 \
  --print-sample
```

运行结束后，可以在 `code/results/` 下看到自动生成的 `custom_*.json` 结果文件。

### 3.3 集群实验示例（Master 环境）

在 Master 节点上（假设脚本中的 `--master` 已设为 `spark://172.23.166.104:7078`）：

```bash
cd ~/spark-partition-strategy

# 倾斜分布 + 热点 key=0，比较常用配置
code/scripts/run_custom_basic.sh \
  --input-type skewed \
  --num-records 10000000 \
  --num-partitions 128 \
  --hot-keys "0" \
  --hot-bucket-factor 4
```

每次运行结束后：

- 控制台可以看到 Custom 分区前后分布、时间指标；
- `logs/spark-events/` 中会新增一个 event log；
- `code/results/` 中会自动新增一个 `custom_*.json` 结果文件。

---

## 4. 日志解析与结果导出

Custom 实验与 Range 共用同一套：

- `code/tools/parse_spark_eventlog.py`：解析单个 event log；
- `code/tools/collect_latest_eventlog.py`：自动选取最新日志并调用解析脚本。

`run_custom_basic.sh` 在 Spark 任务跑完后，会根据命令行参数自动构造一个 tag（如 `skewed_hot0_b4_p128_10m`），并调用：

```bash
python3 code/tools/collect_latest_eventlog.py \
  custom \
  --event-dir logs/spark-events \
  --results-dir code/results \
  --tag "${TAG}"
```

最终在 `code/results/` 下生成类似：

```text
custom_skewed_hot0_b4_p128_10m_20251201_231045.json
```

其中包含与 Range 相同结构的指标字段，方便同学 D 直接做对比分析。

---

## 5. 推荐的对比实验设计

为了展示自定义分区器在“倾斜优化、均匀不退化”上的效果，推荐至少进行以下几组实验：

1. **均匀数据场景（baseline 验证）**
   - Range（或 Hash）+ `input-type=uniform`；
   - Custom + `input-type=uniform` + `--hot-keys ""`（无热点）。  
   预期：分区分布和 Job/Stage/Task 指标相近，无明显退化。
2. **倾斜数据场景（有/无热点优化对比）**
   - Range（或 Hash）+ `input-type=skewed`；
   - Custom + `input-type=skewed` + `--hot-keys ""`（不启用热点优化）；
   - Custom + `input-type=skewed` + `--hot-keys "HOT_KEY"` + 合适的 `--hot-bucket-factor`。  
   预期：  
   - 不指定热点时，Custom 与 Hash 类似，会出现明显倾斜；  
   - 指定热点后，分区分布更均匀，tail task（p99/max）有所改善。
3. **结果收集**
   - 所有实验中产生的 `range_*.json` 和 `custom_*.json` 统一放在 `code/results/`；
   - D 同学可以基于这些 JSON 画出不同策略在：
     - Job / Stage 时长；
     - Shuffle 读写量；
     - Task 时长分布（尤其 tail）  
     上的差异。

## 6. 推荐的实验工作流（Range / Hash / Custom 对比）

这一节给出一套完整的对比实验流程，目标是：

- 在 **均匀数据** 下，三种策略表现接近，Custom 不明显退化；
- 在 **温和倾斜** 下，Custom 至少不比 Hash 差，甚至略有优势；
- 在 **极端倾斜** 下，Custom 能通过合理的拆桶配置明显优于 Hash。

下面的命令都假设你在项目根目录 `/home/xxx/spark-partition-strategy` 下执行。

### 6.1 均匀场景：uniform, p16, 2m

1. **运行 Hash / Range / Custom**

   ```bash
   # Hash, uniform, 2m, p16
   code/scripts/run_hash_basic.sh \
     --input-type uniform \
     --num-records 2000000 \
     --num-partitions 16

   # Range, uniform, 2m, p16
   code/scripts/run_range_basic.sh \
     --input-type uniform \
     --num-records 2000000 \
     --num-partitions 16

   # Custom, uniform, 2m, p16（无热点，无需拆桶，可以用 b1 或 4 做 sanity check）
   code/scripts/run_custom_basic.sh \
     --input-type uniform \
     --num-records 2000000 \
     --num-partitions 16 \
     --hot-keys "" \
     --hot-bucket-factor 1
   ```

   这组三个 JSON 大致会是：

   - `hash_uniform_p16_2m_*.json`
   - `range_uniform_p16_2m_*.json`
   - `custom_uniform_hotNone_b1_p16_2m_*.json`

2. **预期现象**

   - 三者的 `task_p90_ms` / `task_p99_ms` 差别不大；
   - Custom 在均匀数据下不会明显慢于 Hash / Range。

### 6.2 温和倾斜场景：skewed, p64, 5m（对比 Hash / Range / Custom-b8）

这里假设 `SKEW_RATIO` 在 0.7~0.8 左右，生成 500 万条记录、64 个分区。

1. **运行 Hash / Range / Custom（b8）**

   ```bash
   # Hash, skewed, 5m, p64
   code/scripts/run_hash_basic.sh \
     --input-type skewed \
     --num-records 5000000 \
     --num-partitions 64

   # Range, skewed, 5m, p64
   code/scripts/run_range_basic.sh \
     --input-type skewed \
     --num-records 5000000 \
     --num-partitions 64

   # Custom, skewed, 5m, p64，单热点 key=0，适中拆桶 b8
   code/scripts/run_custom_basic.sh \
     --input-type skewed \
     --num-records 5000000 \
     --num-partitions 64 \
     --hot-keys "0" \
     --hot-bucket-factor 8
   ```

   生成的文件名大致为：

   - `hash_skewed_p64_5m_*.json`
   - `range_skewed_p64_5m_*.json`
   - `custom_skewed_hot0_b8_p64_5m_*.json`

2. **预期现象**

   - Hash 在热点分区上可能出现较大的 `task_p99_ms`；
   - Range 会比 Hash 略好，但不一定完美；
   - Custom-b8 借助拆桶，热点压力摊薄后，`task_p90_ms` / `task_p99_ms` 至少不比 Hash 差，整体 job 时间接近或略优。

### 6.3 极端倾斜场景：skewed, p64, 5m（对比不同桶数 b4 / b8 / b32）

这一组实验我们固定：

- 数据类型：`skewed`
- 记录数：`5m`（500 万）
- 分区数：`64`
- 热点 key：`0`

只改变 Custom 的 `hot-bucket-factor`，对比 `b4` / `b8` / `b32` 三种配置：

```bash
# Custom, skewed, p64, 5m, b4
code/scripts/run_custom_basic.sh \
  --input-type skewed \
  --num-records 5000000 \
  --num-partitions 64 \
  --hot-keys "0" \
  --hot-bucket-factor 4

# Custom, skewed, p64, 5m, b8
code/scripts/run_custom_basic.sh \
  --input-type skewed \
  --num-records 5000000 \
  --num-partitions 64 \
  --hot-keys "0" \
  --hot-bucket-factor 8

# Custom, skewed, p64, 5m, b32（更激进拆桶，接近 num-partitions）
code/scripts/run_custom_basic.sh \
  --input-type skewed \
  --num-records 5000000 \
  --num-partitions 64 \
  --hot-keys "0" \
  --hot-bucket-factor 32
```

对应的结果文件名大致为：

- `custom_skewed_hot0_b4_p64_5m_*.json`
- `custom_skewed_hot0_b8_p64_5m_*.json`
- `custom_skewed_hot0_b32_p64_5m_*.json`

你可以先单独比较 Hash vs Range vs Custom-b32，验证在极端倾斜 + 适当拆桶下 Custom 能否显著优于 Hash；
再横向比较 Custom-b4 / b8 / b32，观察拆桶程度对 `task_p90_ms` / `task_p99_ms` 和 job 总时间的影响。

---

## 7. `hot_bucket_factor` 的推荐参数区间

为了方便做对比实验，同时保证文件名中的 `bX` 与实际使用的桶数一致，当前实现中：

- `hot_bucket_factor` 完全由命令行参数控制，不会在代码中被自动放大或修改；
- 在 `skewed` 场景下，如果未显式传入 `--hot-keys`，则默认将数据生成时的 `HOT_KEY` 视为唯一热点 key。

在实际实验中，建议按以下思路选择 `hot_bucket_factor`：

1. **均匀数据 / 无明显热点（`--input-type=uniform` 或 `--hot-keys=""`）**
   - 建议使用较小的桶数，例如：
     - `hot-bucket-factor = 1`（不拆桶，几乎等价于普通 Hash）；
     - 或 `hot-bucket-factor = 2 ~ 4`（轻微拆桶，用于验证不会对均匀数据产生明显负面影响）。

2. **温和倾斜（例如 `SKEW_RATIO ≈ 0.7 ~ 0.8`，单一热点 key）**
   - 推荐让 `hot_bucket_factor` 与分区数保持同一量级，但可以适当偏小：
     - 例如：`num-partitions = 16` 时，可选 `hot-bucket-factor = 4, 8, 16`；
   - 实验时可以同时跑几组不同的 b 值：
     - `b4`：较保守的拆桶；
     - `b8`：中等拆桶；
     - `b16`：更激进的拆桶；
   - 通过对比 `partition_distribution_*` 和 task/job 时间指标，观察拆桶程度对倾斜缓解效果与开销的影响。

3. **极端倾斜（例如 `SKEW_RATIO ≥ 0.9`，单一热点 key，分区数较大，如 32/64）**
   - 为了让热点 key 的流量尽可能均匀地摊到所有分区，推荐：
     - `hot-bucket-factor` 取值接近或等于 `num-partitions`：
       - 如 `num-partitions = 32`，可选 `b32`；
       - 如 `num-partitions = 64`，可选 `b32` 或 `b64`；
   - 同时也可以留一组偏小的 b（如 `b4` 或 `b8`）对比，展示“拆桶不足时 Custom 无法完全发挥优势”的情况。

4. **多热点场景（如果以后扩展到多个热点 key）**
   - 一般需要考虑“热点 key 个数 × hot_bucket_factor ≈ num-partitions”的平衡：
     - 总桶数远大于分区数时，存在多个桶映射到同一分区的折叠现象，拆桶收益会递减；
   - 当前实验脚本主要面向单热点场景，若后续扩展到多热点，可以在文档中进一步细化推荐区间。

在所有这些实验里，由于 `hot_bucket_factor` 不再被代码自动放大，你可以通过结果文件名中的 `bX`（例如 `custom_skewed_hot0_b4_p64_5m_...json`）直接判断当次实验实际使用的桶数，方便汇总和画图分析。

   - `job_count` / `stage_count` / `task_count`
   - `job_durations_ms` / `stage_durations_ms`
   - `total_shuffle_read_bytes` / `total_shuffle_write_bytes`
   - `task_duration_stats_ms`（min / p50 / p90 / p99 / max）

5. **与 Range / Hash 结果对比**

   将本次 Custom 结果与对应配置下的 Range / Hash 结果并列放在表格中（全部在 `code/results/` 下）：

   - 对比 Job / Stage 总执行时间，观察是否缓解 tail task；
   - 对比 Shuffle Read / Write 字节数；
   - 对比 Task duration 的 p90 / p99 / max，观察自定义分区对热点 key 的优化效果。

   这部分对比分析主要由同学 D 完成，本说明文档的目标是保证 A / C 同学能稳定复现、产出所需的 JSON 指标文件。

   **建议 D 同学在分析报告中明确给出：**

   - Range vs Custom 在倾斜场景下的 Job/Stage 时间对比图；
   - Task duration p90/p99 的对比条形图；
   - 分区分布的箱线图或表格。

---

## 7. 总结

通过 Range 与 Custom 两套实验脚本和自动日志解析工具，本项目已实现：

- 统一的数据生成方式；
- 一致的指标采集手段（Spark event log + JSON 导出）；
- 可复现、可对比的实验流程，方便从“倾斜优化”和“均匀场景退化”两个维度分析自定义 Partitioner 的效果。