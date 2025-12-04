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

## 2. hash_basic.py：核心逻辑与算子链

`code/jobs/hash_basic.py` 实现了基于 **HashPartitioner** 的基准测试。为了与 Range / Custom 两个实验做**公平对比**，三个作业在算子链上保持完全一致，统一为：

> `Parquet 数据读取 → (key, value) RDD → 首次重分区/排序 → 第一次 reduceByKey → 第二次 reduceByKey`

对于 Hash 实验，这条流水线具体为：

1. **读取数据**：
  - 通过公共工具函数 `load_rdd_from_parquet` 从 Parquet 加载 `(key, value)` RDD，并用相同的 `num-partitions` 进行初始 repartition，确保三种策略在入口处一致；
2. **Hash 分区阶段**：
  - 调用 `rdd.partitionBy(numPartitions=...)`，在 PySpark 中默认使用 `portable_hash` 实现 Hash 分区；
  - 记录 `partition_distribution_before` 与 `partition_distribution_after_hash`，并通过调用 `compute_partition_distribution` 所花时间近似统计 `t_partition_shuffle`（即 Hash Shuffle 开销）；
3. **第一次聚合（reduceByKey）**：
  - 在 Hash 分区结果上调用 `reduceByKey`，并通过 `count()` 触发 Action，得到 `t_agg`（聚合阶段耗时）；
4. **第二次 reduceByKey（用于 Stage 对齐）**：
  - 再次对第一次聚合结果调用 `reduceByKey`，在语义上是幂等的，但在 DAG 中会形成与 Range / Custom 实验结构一致的额外 Stage，便于对比 Job/Stage/Task 指标；
5. **最终分区分布**：
  - 使用 `compute_partition_distribution` 记录 `partition_distribution_after_reduce` 和 `partition_distribution_final`，字段含义与 Range / Custom 输出对齐。

**支持的命令行参数（新版）**：

- `--input-path`：Parquet 输入路径，由 `run_hash_basic.sh` 根据数据集名称自动推导；
- `--input-type`：仅作为日志标签（例如 `uniform` / `skewed`），方便在 Spark UI 与结果文件名中识别；
- `--num-partitions`：Hash 分区数（与 Range / Custom 一致）；
- `--print-sample`：是否打印部分结果（调试用）。

**控制台输出的关键指标（新版）**：

- `t_partition_shuffle`：计算 `partition_distribution_after_hash` 所需时间（近似 Hash Shuffle 阶段耗时）；
- `t_agg`：第一次 `reduceByKey` 的总耗时；
- `t_total`：`t_partition_shuffle + t_agg` 的近似和；
- `partition_distribution_before`：初始 RDD 各分区记录数；
- `partition_distribution_after_hash`：Hash 分区后各分区记录数（观察倾斜的关键指标）；
- `partition_distribution_after_reduce` / `partition_distribution_final`：第一次 / 第二次 reduce 后的分区分布。

**事件日志**：

Hash 实验通过 `SparkConf` 开启 `spark.eventLog.enabled`，运行结束后会在 `logs/spark-events/` 生成 event log。后续由统一的 `parse_spark_eventlog.py` 解析 Job / Stage / Task 级别指标，并写入 `code/results/hash_*.json`，从而在 Job 总时间、Stage 划分、Shuffle 字节数、Task 分布等维度与 Range / Custom 做**一一对应的对比**。

---

## 3. 使用 run_hash_basic.sh 运行实验

### 3.1 脚本位置与作用

- 路径：`code/scripts/run_hash_basic.sh`  
- 作用：在本地 / Master 上快速运行 `hash_basic.py` 并**自动导出指标 JSON**。

脚本核心逻辑：

1. 使用 `spark-submit` 运行 `code/jobs/hash_basic.py`；
2. 程序结束后调用 `code/tools/collect_latest_eventlog.py`，自动找到 `logs/spark-events/` 下**最新**的 event log；
3. 再调用 `parse_spark_eventlog.py`，将该日志解析成一个 JSON，写入 `code/results/`，文件名前缀为 `range_...`。


### 3.2 集群实验示例（与 Range / Custom 对齐）

在 Master 节点上（假设项目路径为 `~/spark-partition-strategy`，且脚本中的 `MASTER` 已设为 `spark://...`）：

```bash
cd ~/spark-partition-strategy

# 均匀分布（uniform, 2m, p16）
code/scripts/run_hash_basic.sh \
  --data-type uniform \
  --num-records 2000000 \
  --skew-ratio 0.0 \
  --num-partitions 16

# 倾斜分布（skewed, 5m, p64, 温和倾斜或极端倾斜由 skew-ratio 控制）
code/scripts/run_hash_basic.sh \
  --data-type skewed \
  --num-records 5000000 \
  --skew-ratio 0.75 \
  --num-partitions 64
```

每次运行结束后：

- 控制台可以看到 `hash_basic.py` 打印的时间与分区分布指标；
- `logs/spark-events/` 中会新增一个 event log；
- `code/results/` 中会自动新增一个 `hash_*.json` 结果文件，命名中带有 `data_type` / `records_tag` / `ratio` / `pXX` 等信息，便于与 Range / Custom 的结果做一一对应的对比。

---