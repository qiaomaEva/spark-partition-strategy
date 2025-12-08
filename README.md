# Spark 分区策略实验（Hash / Range / Custom）

本项目旨在深入研究分布式计算框架 Spark 中的 **分区策略（Partitioning Strategy）** 对作业执行性能的影响。通过对比 HashPartitioner、RangePartitioner 以及基于加盐（Salting）机制的自定义 Partitioner，探究其在不同数据分布下的表现。

目前已完成：

- 基于 **500万条（5m）合成数据** 的全流程对比实验；
- 在 **3 台云服务器组成的 Spark Standalone 集群** 上复现了真实的数据倾斜场景；
- 结合 **DAG 可视化** 与 **Task 时间线（Timeline）** 对实验结果进行了定性与定量分析。

------



## 1. 仓库结构



```
spark-partition-strategy/
├── code/
│   ├── data_gen/
│   │   └── DataGen.py             # 数据生成脚本 (Spark SQL -> Parquet)
│   ├── jobs/
│   │   ├── hash_basic.py          # Hash 分区器实验源码
│   │   ├── range_basic.py         # Range 分区器实验源码
│   │   └── custom_basic.py        # 自定义分区器(加盐)实验源码
│   ├── scripts/                   # 自动化运行脚本
│   ├── results/                   # 实验指标数据 (JSON)
│   └── tools/                     # 指标解析与绘图工具
├── logs/                          # Spark event logs
├── docs/
│    └── figures/                  # 实验分析图表
├── run_all_experiments.sh         # 【核心】全流程自动化入口脚本
└── README.md                      # 本文件
```



## 2. 实验环境

### 2.1 集群硬件环境

实验在 3 台阿里云 ECS 上完成，组建 Spark Standalone 集群：

- **Master + Worker-1** (Node A)
- **Worker-2** (Node B)
- **Worker-3** (Node C)

**配置**：vCPU 2-4 核 / 内存 4-8 GiB / Ubuntu 22.04 LTS / 内网互通。

### 2.2 软件环境

- **Spark**: 3.5.7
- **JDK**: OpenJDK 11
- **Python**: 3.10 (Driver 端)

------



## 3. 实验负载设计 (Workload Design)

为了准确评估分区策略对分布式计算性能的影响，本实验在负载设计上遵循以下原则：

### 3.1 核心算子：reduceByKey (Global Aggregation)

实验选取 **基于 Key 的全局聚合操作** 作为标准负载。

- **代码实现**：rdd.reduceByKey(lambda x, y: x + y)
- **逻辑含义**：对数据集按 Key 分组并累加 Value（模拟 WordCount 或 流量统计场景）。

### 3.2 设计意图

1. **强制触发 Shuffle (宽依赖)**：
   reduceByKey 是典型的宽依赖（Wide Dependency）算子。Spark 必须将集群中所有节点上相同 Key 的数据通过网络传输（Shuffle）汇聚到同一个 Executor 节点进行计算。*只有触发了 Shuffle，分区器的分发策略（是按哈希分、按范围分、还是自定义分）才会真正决定数据的去向，从而暴露倾斜问题。*
2. **制造计算与 I/O 瓶颈**：
   在数据倾斜场景下，特定 Key（如 Key=0）的数据量巨大。聚合操作迫使单一 Executor 承担超额的 CPU 计算（累加）和 内存压力（维护聚合哈希表），从而模拟真实的单点故障风险。

### 3.3 数据集构造

- **数据量**：**500 万条 (5m)** 记录。

- **Uniform (均匀)**：Key 在 [0, 1000) 均匀分布，作为 Baseline。

- **Skewed（倾斜）**：

  - **温和倾斜 (0.75)**：热点 Key 占 75% 数据。
  - **极端倾斜 (0.95)**：热点 Key 占 95% 数据。

  ------

## 4. 运行方式

项目提供了一键自动化脚本，按顺序执行：数据生成 -> 运行 Hash/Range/Custom 实验 -> 汇总数据 -> 绘图。

```
cd ~/spark-partition-strategy
# 后台运行全流程实验
nohup ./run_all_experiments.sh > experiment.log 2>&1 &
```

------



## 5. 实验结果与分析

本实验通过对比分析，回应了以下三个核心探究任务：



### 5.1 探究 DAG 调度与 Stage 划分机制

通过观察 Spark UI 的 DAG Visualization，我们发现不同策略导致了截然不同的 DAG 结构：

- **Hash / Range 策略（Baseline）**：

  - **DAG 结构**：Stage 0 (Map) -> **Exchange (Shuffle)** -> Stage 1 (Reduce)

  - **机制分析**：Spark 默认采用一次性 Shuffle 将数据分发到下游。这种“直肠子”式的调度机制在面对倾斜数据时，无法感知热点，机械地将所有热点数据发往同一个 Partition，导致下游 Stage 出现严重的长尾。
   <img width="2541" height="1240" alt="hash" src="https://github.com/user-attachments/assets/9ccd1201-e61e-4f4d-8afb-7fd0923b9960" />


- **Custom 策略（Optimization）**：

  - **DAG 结构**：Stage 0 -> **Exchange** -> Stage 1 -> **Exchange** -> Stage 2
  - **机制分析**：引入了“两阶段聚合”。
    - **第一阶段**：通过加盐（Salting）改变 Key 的分布，触发第一次 Shuffle，实现数据的均匀打散和局部预聚合（Local Reduce）。
    - **第二阶段**：去盐还原 Key，触发第二次 Shuffle，进行极少量的全局聚合。
  - **结论**：虽然 DAG 更复杂、Stage 更多，但这种机制成功将单点压力转化为分布式计算。
     <img width="1885" height="853" alt="custom" src="https://github.com/user-attachments/assets/4349de8c-77e8-4f72-9eba-274c7fcdb241" />


### 5.2 对比 Hash、Range、Custom 三种策略在不同数据分布下的表现

基于 500万 数据集的实验数据对比，我们观察到了有趣的性能分化：

- **在均匀分布（Uniform）下**：三种策略的 Task P99 延迟与 Job 总耗时 **基本持平**。

  **结论**：Custom 策略虽然逻辑复杂，但在数据均匀时引入的额外开销（Overhead）非常小，具备通用性。

- **在温和倾斜（Skewed 0.75）下**：

  - **现象**：Hash 策略开始出现轻微的长尾，但未造成严重阻塞。
  - **Custom 策略表现**：Custom-b8（8个桶）与 Custom-b32（32个桶）均消除了长尾。
  - **关键发现**：在此场景下，**b8 的表现略优于 b32**。这说明在倾斜不极端时，过度拆分（Over-partitioning）带来的聚合开销（Overhead）可能会抵消负载均衡带来的收益。

- **在极端倾斜（Skewed 0.95）下**：

  - **Hash 策略**：表现最差。95% 的数据涌向单一节点，导致严重的单点瓶颈，**总耗时最长**。

  - **Custom 策略**：**表现最优**。Custom-b32 展现了强大的削峰能力，将 P99 延迟压至最低，**总耗时显著低于 Hash 策略**。

  - **结论**：倾斜越严重，Custom 策略的收益越高，足以覆盖其多一轮 Shuffle 的成本。

    

### 5.3 验证自定义分区器（加盐/分桶）在缓解倾斜问题上的有效性

实验数据强有力地验证了 Custom 策略的有效性及参数敏感性：

- **负载均衡验证（Timeline）**：Hash 策略的 Timeline 只有一条极长的绿条，集群资源严重闲置。Custom 策略的 Timeline 呈现整齐密集的短绿条，所有 Executor 并行度极高。
- **有效性结论**：
  - **消除长尾**：Custom 策略通过应用层介入，将物理上的单点热点逻辑化解。
  - **参数权衡 (Trade-off)**：加盐的桶数（Bucket Factor）并非越大越好。极端倾斜（0.95）时，需要 **大桶数 (b32)** 来充分打散热点；温和倾斜（0.75）时，**适中桶数 (b8)** 即可平衡负载与开销，如果桶数过大反而可能增大开销。



## 6. 分工说明

- 王妍匀：
  - 搭建与维护 Spark 集群（Master + Worker-1）并设计数据均匀/倾斜分布场景
  - 协调其他组员部署各自 Worker
  - 统一在 Master 上提交实验作业，收集日志与 JSON 指标
  - 整理并更新本 `README.md` 和基础脚本
- 张滨：
  - 负责 HashPartitioner 部分实验代码（`code/jobs/hash_*`）
  - 编写并维护 Hash 实验脚本（`code/scripts/run_hash_*.sh`）
  - 在自己的服务器上部署 Worker-2，并接入 Master 集群
- 孙佳杰：
  - 负责 RangePartitioner 与自定义 Partitioner 代码（`code/jobs/range_*`, `code/jobs/custom_*`）
  - 编写对应运行脚本（`code/scripts/run_range_*.sh`, `code/scripts/run_custom_*.sh`）
  - 编写并维护自动化日志解析工具（`code/tools/`）
  - 优化实验设计与对应脚本维护（统一实验算子链、参数解析模式）
- 沈丁：
  - 在自己的服务器上部署 Worker-3，并接入 Master 集群
  - 负责绘图与可视化分析（`docs/figures/`）
  - 制作实验答辩PPT
