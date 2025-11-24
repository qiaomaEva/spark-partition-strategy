# Spark 分区策略研究

## 1. 研究目的

探索不同分区策略对 Spark DAG 调度与任务执行的影响。

## 2. 研究内容

- 深入理解 Spark 的 DAG 调度器，分析分区器（Partitioner）对 Stage 划分的影响。
- 分析 HashPartitioner、RangePartitioner 以及自定义 Partitioner 的优缺点及适用场景。
- 在不同 key 分布（均匀分布与数据倾斜）下，通过调整分区策略提升作业性能。

---

## 3. 实验

### 3.1 实验环境

**硬件环境**

- 物理服务器：1 台
  - CPU：2 × Intel(R) Xeon(R) Gold 6226R @ 2.90GHz  
    （共 32 物理核 / 64 线程，`lscpu` 查询结果）
  - 内存：754 GiB（`free -h` 查询结果）
  - 存储：
    - 系统盘：1.8 TB（划分为 `/`、`/home`、`/boot` 等分区）
    - 数据盘：7.3 TB（挂载于 `/data`）
  - 网络：以太网（具体带宽未进一步区分）
  - 操作系统：Ubuntu 20.04.3 LTS（`/etc/os-release` 查询结果）

**软件环境**

- JDK：OpenJDK 1.8.0_452  
- Spark：3.5.7（Standalone 模式，1 Master + 3 Workers）
- Python：3.10.9（可使用 PySpark 编写作业）
- 其他依赖：根据实际代码使用情况补充（如 pandas、matplotlib 等）

**Spark 集群配置（Standalone）**

- Master
  - URL：`spark://172.23.166.104:7078`
  - Web UI：`http://172.23.166.104:8088`
- Workers：3 个（运行在同一物理服务器上）
  - 每个 Worker 分配：
    - Cores：8
    - Memory：32 GiB

> 下图为 Spark 集群 Web UI 截图（1 Master + 3 Workers）：
>
> ![cluster-overview](assets\cluster-overview.png)

### 3.2 实验负载

（待补充）

- 数据集：
  - 均匀分布 key 的数据集：说明总记录数、key 范围、生成方式。
  - 倾斜分布 key 的数据集：例如 80% 记录集中在 10% 的 key 上，说明生成规则。
- 作业类型：
  - 如 `reduceByKey` / `aggregateByKey` / `join` 等，写明主要逻辑。

### 3.3 实验步骤

（先列步骤，等跑完实验再补截图）

1. 启动 Spark Standalone 集群：
   - `./sbin/start-master.sh`
   - `./sbin/start-worker.sh spark://172.23.166.104:7078`
2. 使用 `code/data_gen/` 中脚本生成不同数据分布的数据集。
3. 使用 `spark-submit` 提交以下作业（位于 `code/jobs/`）：
   - 默认 `HashPartitioner`
   - `RangePartitioner`
   - 自定义 `Partitioner`
4. 在以下场景下分别运行并记录指标：
   - 均匀分布数据 + 不同分区策略
   - 倾斜分布数据 + 不同分区策略
5. 通过 Spark Web UI 和日志记录：
   - 作业执行时间
   - Shuffle Read / Shuffle Write 数据量
   - Stage 与 Task 数量，以及 Task 负载分布情况

### 3.4 实验结果与分析

（等拿到数据后填表格+画图）

- 表格呈现不同场景下的执行时间 / Shuffle 数据量。
- 使用折线图 / 柱状图对比：
  - 不同分区策略在相同数据分布下的表现差异。
  - 同一分区策略在均匀 vs 倾斜数据下的性能退化情况。
- 分析原因：
  - 数据倾斜导致少数 Task 成为瓶颈。
  - 自定义 Partitioner 是否缓解了热点问题。

---

## 4. 结论

（实验结束后总结）

- 归纳不同分区策略的优势和适用场景。
- 给出在实际业务中选择分区策略的建议。

---

## 5. 分工

- **同学 A**
  - 负责 Spark 集群环境搭建与维护（1 Master + 3 Workers）
  - 建立 GitHub 仓库，编写README文档
  - 设计实验数据集（均匀 / 倾斜 key 分布）
  - 在服务器上执行所有实验脚本，收集运行日志与性能指标
- **同学 B**
  - 编写数据生成脚本（`code/data_gen/`）和 HashPartitioner 实验作业代码（`code/jobs/hash_*`）
  - 编写对应的集群运行脚本（`code/scripts/run_hash_*.sh`）
- **同学 C**
  - 编写 RangePartitioner 实验作业代码（`code/jobs/range_*`）
  - 设计并实现自定义 Partitioner 实验（`code/jobs/custom_*`）
  - 编写对应的集群运行脚本（`code/scripts/run_range_*.sh`、`code/scripts/run_custom_*.sh`）
- **同学 D**
  - 负责整理 `code/results/` 中的结果数据，完成可视化与统计分析
  - 撰写 “实验结果与分析” 与 “结论” 部分文档

> 贡献度排序：A ≈ C > B ≈ D

