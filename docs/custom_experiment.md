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

## 2. custom_basic.py：核心逻辑与算子链

`code/jobs/custom_basic.py` 实现了一个**针对倾斜数据优化，同时兼顾均匀数据**的自定义分区实验。为了与 Hash / Range 两个实验在算子链上保持完全一致，Custom 作业同样遵循统一的流水线：

> `Parquet 数据读取 → (key, value) RDD → 首次重分区/排序 → 第一次 reduceByKey → 第二次 reduceByKey`

与 Hash / Range 的区别只在于“首次重分区”的实现方式：这里使用自定义的 skew-aware partitioner 和热点拆桶逻辑，其余聚合/还原步骤保持一致，确保 Job/Stage/Task 指标的对比是公平的。

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

### 2.2 输入参数（与 Hash / Range 对齐）

Custom 作业使用从 Parquet 读取的 `(key, value)` 数据，与 Hash / Range 共用同一批数据文件（由 `code/data_gen/DataGen.py` 生成），因此命令行参数也与另外两个实验对齐：

- `--input-path`：Parquet 输入路径，由 `run_custom_basic.sh` 根据 `data-type` / `num-records` / `skew-ratio` / `num-partitions` 自动推导；
- `--input-type`：日志标签（例如 `uniform` / `skewed`），同时也写入 Spark 应用名称与结果文件名；
- `--num-partitions`：逻辑分区数（自定义分区前后都保持一致，与 Hash / Range 使用相同的值）；
- `--hot-keys`：逗号分隔的热点 key 列表，例如 `"0"` 或 `"0,1,2"`；
  - 若在 `--input-type=skewed` 场景下不指定（或传空字符串），程序会自动将数据生成器中使用的 `HOT_KEY`（默认 0）视为唯一热点 key；
- `--hot-bucket-factor`：每个热点 key 拆成多少个子桶位（例如 4、8、32）。**该值不会被程序自动修改，实际使用的桶数与命令行保持一致**；
- `--print-sample`：是否打印部分结果，仅本地调试使用。

### 2.3 自定义算子链与输出指标（stdout）

在 Custom 实验中，统一的算子链各阶段含义如下：

1. **Parquet 读取与初始分区**：
   - 通过 `load_rdd_from_parquet` 加载 `(key, value)` RDD，并重分区到指定的 `num-partitions`，与 Hash / Range 保持一致；
   - 记录 `partition_distribution_before` 作为对比基线；
2. **自定义分区阶段（partitionBy + 热点拆桶）**：
   - 首先通过 `wrap_keys_for_custom_partitioner` 将每条记录的 key 包装为 `(orig_key, sub_bucket)`，其中：
     - 对于热点 key：根据 `hot-bucket-factor` 将记录映射到多个子桶；
     - 对于非热点 key：`sub_bucket` 固定为 0；
   - 然后调用 `partitionBy(numPartitions, partitionFunc=custom_partition_func(...))`，自定义分区函数根据 `(orig_key, sub_bucket)` 与热点拆桶策略决定物理分区；
   - 通过 `compute_partition_distribution` 记录 `partition_distribution_after_custom`，并统计 `t_partition`（近似包括 shuffle write/read 的时间）；
3. **第一次 reduceByKey（在包装 key 上聚合）**：
   - 对 `( (orig_key, sub_bucket), value )` 调用 `reduceByKey`，得到 `aggregated_rdd`，并通过 `count()` 触发 Action；
   - 记录 `partition_distribution_after_reduce_raw` 以及聚合耗时 `t_agg`；
4. **第二次 reduceByKey（还原原始 key，结构对齐）**：
   - 通过 `unwrap_keys_after_agg` 将 key 从 `(orig_key, sub_bucket)` 还原为 `orig_key`，并再次 `reduceByKey`，得到最终的 `(key, sum)` 结果 RDD；
   - 记录 `partition_distribution_final`，其含义与 Hash / Range 的最终分布完全一致；
5. **总耗时**：
   - `t_total = t_partition + t_agg`，可与 Hash 的 `t_partition_shuffle + t_agg`、Range 的 `t_sort + t_agg` 直接对比。

运行时，程序会首先打印本次实验的关键参数，例如：

```text
===== Custom Partitioner Basic Experiment (by C) =====
Input path       : file:///home/xxx/spark-data/skewed_5m_ratio0.75
Input type       : skewed
Num partitions   : 64
Hot keys         : 0
Hot bucket factor: 8
Print sample     : False
```

之后会输出关键信息：

- `t_partition_seconds` / `t_agg_seconds` / `t_total_seconds_approx`；
- `partition_distribution_before` / `partition_distribution_after_custom`；
- `partition_distribution_after_reduce_raw` / `partition_distribution_final`。

同样，`custom_basic.py` 通过 `SparkConf` 开启事件日志，将 Job/Stage/Task 级别指标写入项目根目录下的 `logs/spark-events/`。后续由统一的 `parse_spark_eventlog.py` 解析并写入 `code/results/custom_*.json`，与 Hash / Range 的 JSON 结构完全对齐，方便统一画图与对比分析。

---

## 3. 使用 run_custom_basic.sh 运行实验

### 3.1 脚本位置与作用

- 路径：`code/scripts/run_custom_basic.sh`  
- 作用：在本地 / Master 上快速运行 `custom_basic.py`，并**自动导出 JSON 结果**到 `code/results/`，文件前缀为 `custom_...`。

### 3.2 示例命令（与 Hash / Range 对齐）

在本地或 Master 环境下，可以通过 `run_custom_basic.sh` 运行与 Hash / Range 完全对齐的配置。脚本会根据 `--data-type` / `--num-records` / `--skew-ratio` / `--num-partitions` 自动推导 `--input-path`，并在结果文件名中编码这些信息：

```bash
cd ~/spark-partition-strategy

# 均匀数据 + 无热点（退化为 Hash 行为）
code/scripts/run_custom_basic.sh \
  --data-type uniform \
  --num-records 2000000 \
  --skew-ratio 0.0 \
  --num-partitions 16 \
  --hot-keys 0 \
  --hot-bucket-factor 1

# 倾斜数据 + 单热点 key=0，适中拆桶 b8（skewed, 5m, p64, ratio0.75）
code/scripts/run_custom_basic.sh \
  --data-type skewed \
  --num-records 5000000 \
  --skew-ratio 0.75 \
  --num-partitions 64 \
  --hot-keys 0 \
  --hot-bucket-factor 8
```

每次运行结束后：

- 控制台会输出自定义分区前后、聚合前后各阶段的时间与分区分布；
- `logs/spark-events/` 中会新增一个 event log；
- `code/results/` 中会自动新增一个 `custom_*.json` 结果文件，文件名中包含数据类型、规模、倾斜度（ratio）、热点配置与桶数（hot/bX）以及分区数，便于与 Hash / Range 的结果做统一的对比分析。

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