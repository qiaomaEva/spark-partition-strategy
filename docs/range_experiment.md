# Range 分区实验说明（同学 C）

本文档说明如何在本项目中复现 **RangePartitioner 实验** 的完整流程，包括：

- 如何调用 `run_range_basic.sh` 运行实验；
- Spark 如何将事件日志写入 `logs/spark-events`；
- 如何使用 `code/tools/collect_latest_eventlog.py` / `parse_spark_eventlog.py` 解析日志并生成 `code/results/*.json` 结果文件。

> 适用对象：主要面向在 Master 节点上跑实验的同学 A / D，以及需要复现实验的同学。  
> 省流：直接看 **6. 推荐的实验工作流（Range 分区）**。

---

## 1. 实验脚本与相关文件

Range 实验主要涉及以下文件：

- 实验作业代码（PySpark）  
  - `code/jobs/range_basic.py`
- 运行脚本（Shell）  
  - `code/scripts/run_range_basic.sh`
- Spark 事件日志目录  
  - `logs/spark-events/`
- 事件日志解析工具（Python）  
  - `code/tools/parse_spark_eventlog.py`
  - `code/tools/collect_latest_eventlog.py`
- 解析后结果输出目录  
  - `code/results/`

目录结构大致为：

```text
spark-partition-strategy/
  code/
    jobs/
      range_basic.py
      custom_basic.py
    scripts/
      run_range_basic.sh
      run_custom_basic.sh
    tools/
      parse_spark_eventlog.py
      collect_latest_eventlog.py
    results/
      # 解析出的 JSON 结果文件
  logs/
    spark-events/
      # Spark event log 原始日志
```

---

## 2. range_basic.py：核心逻辑与指标

`code/jobs/range_basic.py` 实现了一个基础的 Range-like 分区实验，主要特点：

- 使用 `sortByKey(numPartitions=...)` 模拟 RangePartitioner 行为；
- 支持三种输入类型（**均在 Spark 内部生成，不再依赖 CSV 文件**）：
  - `synthetic`：简单的 `(key, value)` 测试数据，用于功能验证；
  - `uniform`：通过 `sc.range` + `NUM_KEYS` 在 Spark 内部生成均匀分布的 `(key, value)`；
  - `skewed`：通过 `sc.range` + `NUM_KEYS` + `SKEW_RATIO` + `HOT_KEY` 在 Spark 内部生成带热点 key 的倾斜数据；
 支持常用命令行参数：
  - `--input-type {synthetic, uniform, skewed}`：三种数据生成模式（全部在 Spark 内部生成）
  - `--num-records`：三种 input-type 共用，控制生成记录条数
  - `--num-partitions`：控制 RDD / `sortByKey` 的分区数
  - `--print-sample`：是否打印结果样本，仅本地调试使用；
  - `t_sort_seconds`：`sortByKey` 耗时；
  - `t_agg_seconds`：`reduceByKey` 耗时；
  - `t_total_seconds_approx`：两者之和（近似总耗时）；
  - `partition_distribution_before`：原始 RDD 各分区记录数；
  - `partition_distribution_after_sort`：`sortByKey` 后各分区记录数；
  - `partition_distribution_after_reduce`：`reduceByKey` 后各分区记录数。

此外，`range_basic.py` 中通过 `SparkConf` 开启了 **Spark 事件日志**：

```python
# 伪代码示意
conf = (
    SparkConf()
    .setAppName(f"RangePartitionerBasic-{run_id}")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "file://<项目根目录>/logs/spark-events")
)
```

每次运行都会在项目根目录下的 `logs/spark-events/` 生成一个 event log 文件，用于后续解析 Job/Stage/Task 级别的指标。  
所有 README 要求的：

- Job / Stage 总执行时间；
- Shuffle Read / Shuffle Write 数据量；
- Task 运行时间分布（tail task：p90 / p99 / max）；

都通过解析这些 event log 得出，并最终导出到 `code/results/*.json`。

运行时，程序会首先打印本次实验的关键参数，例如：

```text
===== RangePartitioner Basic Experiment (by C) =====
Input type     : uniform
Num records    : 200000
Num partitions : 8
Print sample   : True
```

---

## 3. 使用 run_range_basic.sh 运行实验

### 3.1 脚本位置与作用

- 路径：`code/scripts/run_range_basic.sh`  
- 作用：在本地 / Master 上快速运行 `range_basic.py` 并**自动导出指标 JSON**。

脚本核心逻辑：

1. 使用 `spark-submit` 运行 `code/jobs/range_basic.py`；
2. 程序结束后调用 `code/tools/collect_latest_eventlog.py`，自动找到 `logs/spark-events/` 下**最新**的 event log；
3. 再调用 `parse_spark_eventlog.py`，将该日志解析成一个 JSON，写入 `code/results/`，文件名前缀为 `range_...`。

> 对 A / D 同学来说，只要记住：  
> **“跑 Range = 调 `run_range_basic.sh`，结果 JSON 自动出现在 `code/results/`。”**

### 3.2 本地调试示例（local 模式）

在 WSL / 本地 Linux 环境中，可以先将脚本里的 `--master` 改为 `local[*]`，然后运行小规模测试：

```bash
cd ~/spark-partition-strategy

code/scripts/run_range_basic.sh \
  --input-type synthetic \
  --num-records 200000 \
  --num-partitions 8 \
  --print-sample
```

也可以直接调试内部生成的均匀 / 倾斜数据：

```bash
# uniform 数据（Spark 内部生成）
code/scripts/run_range_basic.sh \
  --input-type uniform \
  --num-records 200000 \
  --num-partitions 8 \
  --print-sample

# skewed 数据（Spark 内部生成，使用 NUM_KEYS / SKEW_RATIO / HOT_KEY）
code/scripts/run_range_basic.sh \
  --input-type skewed \
  --num-records 200000 \
  --num-partitions 8 \
  --print-sample
```

运行结束后，可以在 `code/results/` 下看到自动生成的 `range_*.json` 文件。

### 3.3 集群实验示例（Master 环境）

在 Master 节点上（假设项目路径为 `~/spark-partition-strategy`，且脚本中的 `--master` 已设为 `spark://172.23.166.104:7078`）：

```bash
cd ~/spark-partition-strategy

# 均匀分布大数据示例：
code/scripts/run_range_basic.sh \
  --input-type uniform \
  --num-records 10000000 \
  --num-partitions 128

# 倾斜分布大数据示例：
code/scripts/run_range_basic.sh \
  --input-type skewed \
  --num-records 10000000 \
  --num-partitions 128
```

每次运行结束后：

- 控制台可以看到 `range_basic.py` 打印的基本指标和分区分布；
- `logs/spark-events/` 中会新增一个 event log；
- `code/results/` 中会自动新增一个 `range_*.json` 结果文件。

---

## 4. Spark 事件日志：logs/spark-events

每次执行 `range_basic.py` 时，Spark 会自动生成一个 event log 文件，位于：

```text
<项目根目录>/logs/spark-events/
```

示例：

```bash
ls logs/spark-events
# 可能看到类似：
# app-20251129212316-0005
# local-1710000000000
```

文件名由 Spark 自行生成，内容为多行 JSON，每一行是一个事件（`SparkListenerJobStart`、`SparkListenerJobEnd`、`SparkListenerStageCompleted`、`SparkListenerTaskEnd` 等）。

---

## 5. 日志解析与结果导出：parse_spark_eventlog.py + collect_latest_eventlog.py

### 5.1 parse_spark_eventlog.py：解析一个日志文件

`code/tools/parse_spark_eventlog.py` 负责解析单个 event log 文件，输出一个 JSON，包含以下字段（示例）：

```json
{
  "job_count": 5,
  "stage_count": 7,
  "task_count": 32,
  "job_durations_ms": [
    { "jobId": "0", "duration_ms": 767 },
    { "jobId": "1", "duration_ms": 42 }
  ],
  "stage_durations_ms": [
    { "stageId": "0", "duration_ms": 748 },
    { "StageId": "1", "duration_ms": 41 }
  ],
  "total_shuffle_read_bytes": 0,
  "total_shuffle_write_bytes": 0,
  "task_duration_stats_ms": {
    "min_ms": 22,
    "p50_ms": 35,
    "p90_ms": 586,
    "p99_ms": 587,
    "max_ms": 602,
    "count": 32
  }
}
```

这覆盖了 README 要求的指标：

- Job / Stage 总执行时间；
- Shuffle Read / Shuffle Write 数据量（字节）；
- Task 运行时间分布（tail task：p90 / p99 / max）。

它的底层用法是：

```bash
python3 code/tools/parse_spark_eventlog.py <event_log_path> <output_json_path>
```

### 5.2 collect_latest_eventlog.py：自动选择“最新日志”

`code/tools/collect_latest_eventlog.py` 封装了“找最新日志 + 调用解析脚本”的过程。

用法：

```bash
python3 code/tools/collect_latest_eventlog.py <strategy> [--event-dir LOG_DIR] [--results-dir RESULTS_DIR] [--tag TAG]
```

参数说明：

- `strategy`：策略名称，会成为结果文件前缀，如 `"range"` / `"custom"`；
- `--event-dir`：Spark event log 目录，默认 `logs/spark-events`；
- `--results-dir`：结果输出目录，默认 `code/results`；
- `--tag`：可选标签，用于描述本次实验配置，如 `uniform_p128_10m`、`skewed_p128_10m`。

示例（手动调用时）：

```bash
cd ~/spark-partition-strategy

python3 code/tools/collect_latest_eventlog.py \
  range \
  --event-dir logs/spark-events \
  --results-dir code/results \
  --tag "uniform_p128_10m"
```

执行后，会在 `code/results/` 下生成类似：

```text
code/results/range_uniform_p128_10m_20251201_231045.json
```

在实际使用中，**`run_range_basic.sh` 已经自动调用了这个脚本**，因此手动调用只在需要额外解析时使用。

---

## 6. 推荐的实验工作流（Range 分区）

综合以上，一个完整的 Range 分区实验推荐如下工作流：

1. **在 Master 上拉取最新代码**

   ```bash
   cd ~/spark-partition-strategy
   git pull
   ```

2. **运行 Range 实验（自动导出 JSON）**

   ```bash
   # 均匀分布
   code/scripts/run_range_basic.sh \
     --input-type uniform \
     --num-records 10000000 \
     --num-partitions 128

   # 倾斜分布
   code/scripts/run_range_basic.sh \
     --input-type skewed \
     --num-records 10000000 \
     --num-partitions 128
   ```

3. **确认 Spark Web UI 和控制台输出**

   - Web UI：`http://<master-host>:8088` / `:4040` 上查看对应 Application；
   - 控制台：查看 `range_basic.py` 打印的时间与分区分布信息。

4. **查看并收集 JSON 结果**

   ```bash
   ls code/results
   # 例如：
   # range_uniform_p128_10m_20251201_231045.json
   # range_skewed_p128_10m_20251201_231200.json
   ```

5. **将结果文件交给分析同学（D）**

   - D 同学可以直接基于 `code/results/*.json` 整理表格、绘制图表；
   - 也可对比后续 HashPartitioner / 自定义 Partitioner 的结果。

---

## 7. 后续工作：与其他分区策略的对比

当前 Range 实验已经具备：

- 统一的运行脚本（自动导出 JSON）；
- 完整的指标采集流程（控制台 + event log + JSON）。

后续将为：

- Hash 分区（基线）；
- 自定义 Partitioner（Custom）；

构建类似的实验脚本与结果导出流程，从而在 `code/results/` 中形成可对比的多组 JSON 数据，用于分析不同分区策略对：

- Job/Stage 执行时间；
- Shuffle 读写量；
- Task 时长分布（tail task）；

的具体影响。