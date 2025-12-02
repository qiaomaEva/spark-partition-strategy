# Hash 分区实验说明（同学 B）

本文档说明如何在本项目中复现 **HashPartitioner 实验**（作为性能对比的 Baseline）的完整流程，包括：

- 如何调用 `run_hash_basic.sh` 运行实验；
- Spark 如何将事件日志写入 `logs/spark-events`；
- 如何使用 `code/tools/collect_latest_eventlog.py` / `parse_spark_eventlog.py` 解析日志并生成 `code/results/*.json` 结果文件。

---

## 1. 实验脚本与相关文件

Hash 实验（Baseline）主要涉及以下文件：

- 实验作业代码（PySpark）  
  - `code/jobs/hash_basic.py`
- 运行脚本（Shell）  
  - `code/scripts/run_hash_basic.sh`
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
      hash_basic.py      <-- 本次实验baseline
    scripts/
      run_hash_basic.sh  <-- 运行脚本
    tools/
      parse_spark_eventlog.py
      collect_latest_eventlog.py
    results/
      # 解析出的 hash_*.json 结果文件
  logs/
    spark-events/
      # Spark event log 原始日志
```

---

## 2. hash_basic.py：核心逻辑与指标

`code/jobs/hash_basic.py` 实现了基于 **HashPartitioner** 的基准测试，主要特点：

- 使用 `rdd.partitionBy(numPartitions=...)`，在 PySpark 中默认使用 `portable_hash` 实现 Hash 分区；
- **聚合逻辑**：使用 `groupByKey().mapValues(len)` 模拟聚合过程；
- **数据生成**：支持三种输入类型（**均在 Spark 内部生成**，与 Range 实验保持一致以控制变量）：
  - `synthetic`：简单取模数据，用于功能验证；
  - `uniform`：通过 `sc.range` 生成均匀分布数据；
  - `skewed`：通过 `sc.range` 生成带热点 key 的倾斜数据（由 `SKEW_RATIO` 和 `HOT_KEY` 控制）；

**支持的命令行参数**：

- `--input-type {synthetic, uniform, skewed}`：数据分布模式；
- `--num-records`：生成记录总条数；
- `--num-partitions`：Hash 分区数；
- `--print-sample`：是否打印部分结果（调试用）。

**输出的关键指标**（控制台打印）：

- `t_partition_seconds`：执行 `partitionBy` (Shuffle Write) 的耗时；
- `t_agg_seconds`：执行聚合操作 (Shuffle Read + Compute) 的耗时；
- `t_total_seconds_approx`：近似总耗时；
- `partition_distribution_after_hash`：Hash 分区后各分区的记录数（**观察数据倾斜的关键指标**）。

**事件日志**：

程序通过 `SparkConf` 开启了 `spark.eventLog.enabled`，运行结束后会在 `logs/spark-events/` 生成日志，包含 Job/Stage/Task 级别的详细性能数据。

---

## 3. 使用 run_hash_basic.sh 运行实验

### 3.1 脚本位置与作用

- 路径：`code/scripts/run_hash_basic.sh`  
- 作用：在本地 / Master 上快速运行 `hash_basic.py` 并**自动导出指标 JSON**。

脚本核心逻辑：

1. 使用 `spark-submit` 运行 `code/jobs/hash_basic.py`；
2. 程序结束后调用 `code/tools/collect_latest_eventlog.py`，自动找到 `logs/spark-events/` 下**最新**的 event log；
3. 再调用 `parse_spark_eventlog.py`，将该日志解析成一个 JSON，写入 `code/results/`，文件名前缀为 `range_...`。


### 3.2 集群实验示例（Master 环境）

在 Master 节点上（假设项目路径为 `~/spark-partition-strategy`，且脚本中的 `--master` 已设为 `spark://****`）：

```bash
cd ~/spark-partition-strategy

# 均匀分布大数据示例：
code/scripts/run_hash_basic.sh \
  --input-type uniform \
  --num-records 10000000 \
  --num-partitions 128

# 倾斜分布大数据示例：
code/scripts/run_hash_basic.sh \
  --input-type skewed \
  --num-records 10000000 \
  --num-partitions 128
```

每次运行结束后：

- 控制台可以看到 `hash_basic.py` 打印的基本指标和分区分布；
- `logs/spark-events/` 中会新增一个 event log；
- `code/results/` 中会自动新增一个 `range_*.json` 结果文件。

---