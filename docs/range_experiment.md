# Range 分区实验说明（同学 C）

本文档说明如何在本项目中复现 **RangePartitioner 实验** 的完整流程，包括：

- 如何调用 `run_range_basic.sh` 运行实验；
- Spark 如何将事件日志写入 `logs/spark-events`；
- 如何使用 `code/tools/parse_spark_eventlog.py` 解析日志并生成 `code/results/*.json` 结果文件。

> 适用对象：主要面向在 Master 节点上跑实验的同学 A / D，以及需要复现实验的同学。
>
> 省流：直接看 **6. 推荐的实验工作流（Range 分区）**

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
- 解析后结果输出目录  
  - `code/results/`

目录结构大致为：

```text
spark-partition-strategy/
  code/
    jobs/
      range_basic.py
    scripts/
      run_range_basic.sh
    tools/
      parse_spark_eventlog.py
    results/
      # 解析出的 JSON / CSV 结果文件
  logs/
    spark-events/
      # Spark event log 原始日志
```

---

## 2. range_basic.py：核心逻辑与指标

`code/jobs/range_basic.py` 实现了一个基础的 Range-like 分区实验，主要特点：

- 使用 `sortByKey(numPartitions=...)` 模拟 RangePartitioner 行为；
- 支持三种输入类型：
  - `synthetic`：在内存中生成 `(key, value)` 测试数据；
  - `uniform`：从 `data/uniform_data/*.csv` 读取；
  - `skewed`：从 `data/skewed_data/*.csv` 读取；
- 支持常用命令行参数：
  - `--input-type {synthetic, uniform, skewed}`
  - `--num-records`（仅 synthetic 模式使用）
  - `--num-partitions`
  - `--input-path`（默认 `data`）
  - `--print-sample`（是否打印结果样本，仅本地调试使用）；
- 输出以下指标（直接打印到 stdout）：
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

---

## 3. 使用 run_range_basic.sh 运行实验

### 3.1 脚本位置与作用

- 脚本路径：`code/scripts/run_range_basic.sh`  
- 作用：方便在本地 / Master 节点上快速调用 `range_basic.py`，并传递统一的参数。

典型脚本内容示意（本地调试版）：

```bash
#!/usr/bin/env bash
set -e

cd ~/spark-partition-strategy

spark-submit \
  --master local[*] \
  code/jobs/range_basic.py \
  "$@"
```

在 Master 集群环境中，只需将 `--master` 改为真实的 Spark Master 地址，例如：

```bash
spark-submit \
  --master spark://172.23.166.104:7078 \
  code/jobs/range_basic.py \
  "$@"
```

> 注意：脚本末尾的 `"$@"` 会将所有传入的命令行参数原样转发给 `range_basic.py`，便于灵活控制实验配置。

### 3.2 本地调试示例（local 模式）

在 WSL / 本地 Linux 环境中，可以先用 `local[*]` 做小规模调试：

```bash
cd ~/spark-partition-strategy

code/scripts/run_range_basic.sh \
  --input-type synthetic \
  --num-records 200000 \
  --num-partitions 8 \
  --print-sample
```

或测试文件输入（假设 `data` 下已有对应 CSV）：

```bash
# uniform 数据
code/scripts/run_range_basic.sh \
  --input-type uniform \
  --input-path data \
  --num-partitions 8 \
  --print-sample

# skewed 数据
code/scripts/run_range_basic.sh \
  --input-type skewed \
  --input-path data \
  --num-partitions 8 \
  --print-sample
```

### 3.3 集群实验示例（Master 环境）

在 Master 节点上（假设项目路径为 `~/spark-partition-strategy`，且脚本中的 `--master` 已设为 `spark://172.23.166.104:7078`）：

```bash
cd ~/spark-partition-strategy

code/scripts/run_range_basic.sh \
  --input-type uniform \
  --input-path /data/spark/partition_experiment \
  --num-partitions 128
```

运行结束后，除了在控制台看到 `range_basic.py` 打印的“基本指标”和“分区分布”外，Spark 还会在 `logs/spark-events/` 写入本次 Application 的 event log 文件。

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
# application_1710000000000_0001
# local-1710000000000
```

文件名由 Spark 自行生成，内容为多行 JSON，每一行是一个事件（JobStart、JobEnd、StageCompleted、TaskEnd 等）。

---

## 5. 使用 parse_spark_eventlog.py 解析日志并生成结果

为了从 event log 中提取 README 要求的指标（Job/Stage 时间，Shuffle 读写量，Task 时长分布），项目提供了解析脚本：

- 路径：`code/tools/parse_spark_eventlog.py`

### 5.1 支持的指标

解析脚本会输出一个 JSON，包含以下字段（示例）：

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
    { "stageId": "1", "duration_ms": 41 }
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

这基本覆盖了 README 要求的：

- Job / Stage 总执行时间；
- Shuffle Read / Shuffle Write 数据量（字节）；
- Task 运行时间分布（tail task 分析：p90 / p99 / max）。

### 5.2 使用方法

解析脚本用法：

```bash
python3 code/tools/parse_spark_eventlog.py <event_log_path> <output_json_path>
```

例如：

1. 先找到最新的 event log：

   ```bash
   ls logs/spark-events
   # 假设看到：local-1710000000000
   ```

2. 解析该日志并输出到 `code/results/`：

   ```bash
   python3 code/tools/parse_spark_eventlog.py \
     logs/spark-events/local-1710000000000 \
     code/results/range_uniform_p128_20251127.json
   ```

完成后，`code/results/` 目录下会出现一个 `range_uniform_p128_20251127.json`，其中包含上述指标。该 JSON 可以作为后续分析（画图、对比不同分区策略）的基础数据源。

> 建议：结果文件命名中包含 **策略类型 / 数据类型 / 分区数 / 日期**，例如：
> - `range_uniform_p128_20251127.json`
> - `range_skewed_p128_20251127.json`

---

## 6. 推荐的实验工作流（Range 分区）

综上，一个完整的 Range 分区实验推荐如下工作流：

1. **在 Master 上拉取最新代码**  
   ```bash
   cd ~/spark-partition-strategy
   git pull
   ```

2. **运行 Range 实验（run_range_basic.sh）**  
   例如：
   ```bash
   code/scripts/run_range_basic.sh \
     --input-type uniform \
     --input-path /data/spark/partition_experiment \
     --num-partitions 128
   ```

3. **确认 Spark Web UI 和控制台输出**  
   - Web UI：`http://<master-host>:8088` 上查看对应 Application；
   - 控制台：`range_basic.py` 打印的时间与分区分布信息。

4. **解析 event log，生成结果 JSON 到 code/results/**  
   ```bash
   python3 code/tools/parse_spark_eventlog.py \
     logs/spark-events/<对应的 eventlog 文件> \
     code/results/range_uniform_p128_20251127.json
   ```

5. **将结果文件交给分析同学（D）**  
   - D 同学可以直接基于 `code/results/*.json` 整理表格、绘制图表；
   - 也可对比后续 HashPartitioner / 自定义 Partitioner 的结果。

---

## 7. 后续工作：与其他分区策略的对比

当前 Range 实验已经具备：

- 统一的运行脚本；
- 完整的指标采集流程（控制台 + event log + JSON）；

后续将为：

- Hash 分区（基线）；
- 自定义 Partitioner（Custom）；

构建类似的实验脚本与结果导出流程，从而在 `code/results/` 中形成可对比的多组 JSON 数据，用于分析不同分区策略对：

- Job/Stage 执行时间；
- Shuffle 读写量；
- Task 时长分布（tail task）；

的具体影响。