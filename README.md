# Spark 分区策略研究

## 1. 研究目的

探索不同分区策略对 Spark DAG 调度与任务执行的影响。

## 2. 研究内容

- 深入理解 Spark 的 DAG 调度器，分析分区器（Partitioner）对 Stage 划分的影响。
- 分析 HashPartitioner、RangePartitioner 以及自定义 Partitioner 的优缺点及适用场景。
- 在不同 key 分布（均匀分布与数据倾斜）下，通过调整分区策略提升作业性能。
- 针对数据倾斜场景设计自定义 Partitioner，观察其对尾部 task（tail task）和整体执行时间的影响。

---

## 3. 实验

### 3.1 实验环境

本实验所有结果均在下述 **多机 Spark 集群** 上完成。

#### 3.1.1 集群硬件环境

- 集群节点数：3 台物理服务器  
  - **Master + Worker-1（实验室服务器，负责人：同学 A）**
    - CPU：2 × Intel Xeon Gold 6226R（共 32 核 / 64 线程）
    - 内存：754 GiB
    - 操作系统：Ubuntu 20.04.3 LTS
    
  - **Worker-2（同学 B 的服务器，示例参数，最终按实际填写）**
    - CPU：
    - 内存：
    - 操作系统：
  
  - **Worker-3（同学 D 的服务器，示例参数，最终按实际填写）**
    - CPU：
    - 内存：
    - 操作系统：

#### 3.1.2 软件环境

- 操作系统：各节点均为 Linux（Ubuntu/CentOS）
- JDK：1.8.x
- Spark：3.5.7（Standalone 模式）
- Python：3.10.x（仅 Master 上需要，用于提交 PySpark 作业）

#### 3.1.3 Spark 集群配置

- Master：
  - URL：`spark://172.23.166.104:7078`
  - Web UI：`http://172.23.166.104:8088`
- Worker：
  - Master 节点本机保留 1 个 Worker 进程
  - 同学 B / D 各自在自己的服务器上启动 1 个 Worker
  - 每个 Worker 典型资源配置：2–4 vCores，4–8 GiB 内存

------

### 3.2 实验负载（数据集与作业类型）

#### 3.2.1 数据生成方式

实验使用 **合成数据集**，通过 `code/data_gen/DataGen.py` 使用 Spark 在线生成。
 数据写入路径（在 Master 服务器上）为：

- 均匀分布数据：`data/uniform_data`
- 倾斜分布数据：`data/skewed_data`

数据生成脚本的核心参数：

- 总记录数：`NUM_RECORDS = 10_000_000`（约 1000 万行）
- key 取值范围：`[0, NUM_KEYS)`，其中 `NUM_KEYS = 1000`
- value：长度约 50 字节的字符串，用于模拟一定 payload

在服务器上的生成命令示例：

```
conda activate spark310
cd ~/spark-partition-strategy

spark-submit \
  --master spark://172.23.166.104:7078 \
  code/data_gen/DataGen.py
```

脚本会依次生成均匀分布与倾斜分布两类数据集。

#### 3.2.2 数据分布设定

1. **Dataset A：均匀分布（Uniform）**
   - 总记录数：约 1000 万行
   - key：从 `[0, 1000)` 中均匀采样
   - 生成方式：使用 `rand()` 生成均匀随机数，再映射为整数 key
   - 存储位置：`data/uniform_data/`
2. **Dataset B：数据倾斜（Skewed）**
   - 总记录数：约 1000 万行
   - 热点 key：例如 `key = 0`
   - 倾斯度：约 85% 的记录具有热点 key（`skew_ratio = 0.85`），剩余 15% 均匀分布在 `[1, 999]`
   - 生成方式：
     - 第一部分：`skew_ratio * NUM_RECORDS` 条记录，`key = hot_key`
     - 第二部分：剩余记录，在 `[1, NUM_KEYS - 1]` 范围内均匀采样
   - 存储位置：`data/skewed_data/`

> 实验报告中将通过简单的聚合（如 `groupBy("key").count()`）对两类数据的 key 分布进行可视化，以证明数据倾斜的存在。

#### 3.2.3 作业类型

主要选择对 **分区策略敏感**、且容易产生 Shuffle 的作业类型，包括但不限于：

- `reduceByKey` / `aggregateByKey`：对 `(key, value)` 形式的数据进行聚合
- `groupByKey`（用于对比）：
  - 作为反面示例，展示在倾斜数据 + 不合理分区策略下的性能问题
- 可选扩展：
  - 基于 key 的 join（如与一个小维表 join），进一步观察不同分区策略对 Skew 的影响

所有作业代码统一放在：

- `code/jobs/hash_*`：使用 HashPartitioner 的实验
- `code/jobs/range_*`：使用 RangePartitioner 的实验
- `code/jobs/custom_*`：使用自定义 Partitioner 的实验

------

### 3.3 运行方式与实验步骤

#### 3.3.1 运行方式约定

- **本地 / 组员自测（小规模、方便调试）**

  ```
  spark-submit \
    --master local[*] \
    code/jobs/<experiment>.py
  ```

- **多机集群正式实验（最终结果使用）**

  由同学 A 在 Master 服务器上统一提交：

  ```
  conda activate spark310
  cd ~/spark-partition-strategy
  
  spark-submit \
    --master spark://172.23.166.104:7078 \
    code/jobs/<experiment>.py \
    --input-type {uniform|skewed} \
    --num-records 10000000 \
    [其他参数...]
  ```

  > 实验脚本中不写死 `master` 地址，而是依赖 `spark-submit --master ...` 的配置，使同一份代码既能在本地 `local[*]` 运行，又能在集群上运行。

#### 3.3.2 实验步骤（待补充截图）

1. **启动 Spark 集群**

   - 在 Master 服务器上：

     ```
     $SPARK_HOME/sbin/start-master.sh
     ```

   - 在各 Worker 服务器上（由组员执行）：

     ```
     SPARK_WORKER_CORES=4 \
     SPARK_WORKER_MEMORY=8g \
     $SPARK_HOME/sbin/start-worker.sh spark://172.23.166.104:7078
     ```

2. **生成实验数据（如需离线数据集）**

   - 在 Master 上执行数据生成脚本（见 3.2.1）
   - 或在实验脚本中在线生成同分布的 DataFrame（避免多机文件系统问题）

3. **提交实验作业**

   - 在不同数据分布、不同分区策略下分别运行：
     - 均匀数据 + HashPartitioner / RangePartitioner / 自定义 Partitioner
     - 倾斜数据 + HashPartitioner / RangePartitioner / 自定义 Partitioner
   - 每种配置至少重复多次，取平均值 / 最小值作为结果。

4. **记录与分析指标**

   - 从 Spark Web UI / 日志中记录：
     - Job / Stage 总执行时间
     - Shuffle Read / Shuffle Write 数据量
     - Task 运行时间分布（是否存在极慢的 tail task）
   - 将结果导出至 `code/results/` 目录（例如 CSV / JSON）

5. **绘图与分析**

   - 使用 `code/results/` 中的数据，在 Python / Jupyter Notebook 中绘制：
     - 执行时间对比图（柱状图）
     - Tail Task 时长对比图
     - 不同分区策略的 Shuffle 大小对比图

------

### 3.4 实验结果与分析

（实验完成后填写）
Range 实验详见 `docs/range_experiment.md`
Custom 实验详见 `docs/custom_experiment.md`

- 表格呈现不同场景下的执行时间 / Shuffle 数据量 / tail task 时长。
- 使用图表对比：
  - 不同分区策略在相同数据分布下的表现差异。
  - 同一分区策略在均匀 vs 倾斜数据下的性能退化情况。
- 分析重点：
  - 数据倾斜导致少数 Task 成为瓶颈的原因。
  - 自定义 Partitioner 是否成功将热点 key 拆分 / 单独处理，从而缓解 skew 问题。

------

## 4. 结论

（实验结束后总结）

- 归纳不同分区策略的优势和适用场景。
- 对 HashPartitioner、RangePartitioner 和自定义 Partitioner 在不同数据分布下的表现进行对比总结。
- 针对实际业务场景（例如日志分析、推荐系统特征聚合等），给出选择合适分区策略的建议。

------

## 5. 分工

- **同学 A**
  - 负责 Spark 集群环境搭建与维护（Master 配置、单机开发环境、多机最终集群）
  - 建立 GitHub 仓库，编写并维护 README 文档
  - 设计实验数据集（均匀 / 倾斜 key 分布），编写数据生成脚本（`code/data_gen/`），在服务器上生成 1000 万级数据集
  - 在 Master 服务器上执行所有实验脚本，收集运行日志与性能指标
  - 协调其他同学在各自实验室服务器 / 云主机上部署 Spark Worker，并验证集群状态
- **同学 B**
  - 编写 HashPartitioner 实验作业代码（`code/jobs/hash_*`）
  - 编写对应的集群运行脚本（`code/scripts/run_hash_*.sh`）
  - 在本地 / 自己服务器上使用 `--master local[*]` 对脚本进行功能验证
  - 在自己的实验室服务器 / 云主机上安装 Spark 3.5.7，并启动 Worker 连接 Master，用于多机实验
- **同学 C**
  - 编写 RangePartitioner 实验作业代码（`code/jobs/range_*`）
  - 设计并实现自定义 Partitioner 实验（`code/jobs/custom_*`）
  - 编写对应的集群运行脚本（`code/scripts/run_range_*.sh`、`code/scripts/run_custom_*.sh`）
  - 在本地使用 `--master local[*]` 对脚本进行功能验证
- **同学 D**
  - 负责整理 `code/results/` 中的结果数据，完成可视化与统计分析
  - 撰写 “实验结果与分析” 与 “结论” 部分文档
  - 在自己的实验室服务器 / 云主机上安装 Spark 3.5.7，并启动 Worker 连接 Master，用于多机实验

> 贡献度排序：A ≈ C > B ≈ D