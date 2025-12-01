# Spark 分区策略实验（Hash / Range / Custom）

本项目用于研究 **Spark 分区策略（HashPartitioner / RangePartitioner / 自定义 Partitioner）** 对 DAG 划分、Shuffle、Task 运行时间与尾部任务（tail task）的影响。

目前已完成：

- 基于 **合成数据（均匀 / 倾斜）** 的 Hash / Range / Custom 分区策略实验；
- 在 **3 台云服务器组成的 Spark Standalone 集群** 上跑通所有实验；
- 将实验指标（作业耗时、Task 时延统计等）保存为 JSON，便于后续可视化分析。

------

## 1. 仓库结构

```
spark-partition-strategy/
├── code/
│
│   ├── jobs/
│   │   ├── hash_basic.py          # Hash 分区器实验
│   │   ├── range_basic.py         # Range 分区器实验
│   │   └── custom_basic.py        # 自定义分区器实验
│   ├── scripts/
│   │   ├── run_hash_basic.sh      # Hash 实验运行脚本
│   │   ├── run_range_basic.sh     # Range 实验运行脚本
│   │   └── run_custom_basic.sh    # Custom 实验运行脚本
│ 	├──results/
│   │   ├── hash_uniform_*.json        # Hash + 均匀数据
│   │   ├── hash_skewed_*.json         # Hash + 倾斜数据
│   │   ├── range_uniform_*.json       # Range + 均匀数据
│   │   ├── range_skewed_*.json        # Range + 倾斜数据
│   │   ├── custom_uniform_*.json      # Custom + 均匀数据
│   │   └── custom_skewed_*.json       # Custom + 倾斜数据
│   └── tools/
│       ├── collect_latest_eventlog.py
│       └── parse_spark_eventlog.py
├── logs/                       # Spark event logs
│
├── docs/
│    ├── range_experiment.md        # Range 实验细节
│	 ├── hash_experiment.md         # Hash 实验细节
│    └── custom_experiment.md       # 自定义分区器实验细节
└── README.md                   # 本文件
```

------

## 2. 实验环境

### 2.1 集群硬件环境

本轮实验在 3 台阿里云 ECS 上完成：

- **Master + Worker-1：同学 A 的服务器**
- **Worker-2：同学 B 的服务器**
- **Worker-3：同学 D 的服务器**

每台机器大致配置：

- vCPU：2–4 核
- 内存：4–8 GiB
- 系统盘：≥ 80 GiB
- 操作系统：**Ubuntu 22.04.5 LTS**
- 网络：同一专有网络（VPC）/ 同一安全组，保证内网互通

![cluster-overview](assets\cluster-overview.png)



### 2.2 软件环境

所有节点统一使用：

- OS：Ubuntu 22.04.5 LTS
- JDK：**OpenJDK 11**
- Spark：**3.5.7，Standalone 模式**
- Python（仅 Driver 节点需要，也就是 Master 节点）：**3.10**（conda 环境）

示例安装命令（以 Ubuntu 为例）：

```
# 安装 JDK 11
sudo apt update
sudo apt install -y openjdk-11-jdk

# 下载 Spark 3.5.7
cd ~
wget https://archive.apache.org/dist/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz
tar -xzf spark-3.5.7-bin-hadoop3.tgz
mv spark-3.5.7-bin-hadoop3 ~/spark

# 设置环境变量（追加到 ~/.bashrc）
echo 'export SPARK_HOME=~/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
source ~/.bashrc

# Master 节点额外：Python 环境（用于提交 PySpark 作业）
sudo apt install -y python3-pip  # 或者使用 conda
# 建议使用 conda
# conda create -n spark310 python=3.10 -y
# conda activate spark310
```

------

## 3. Spark 集群部署

### 3.1 Master 节点配置（由 A 同学完成）

1. 确认 Master 节点 **内网 IP**，例如：

   ```
   ip addr show eth0    # 假设网卡名为 eth0
   # 假设输出中内网 IP 为 172.24.49.56
   ```

2. 配置 `spark-env.sh`：

   ```
   cd ~/spark/conf
   cp spark-env.sh.template spark-env.sh
   
   # 根据实际内网 IP 修改
   echo "SPARK_MASTER_HOST=172.24.49.56"   >> spark-env.sh
   echo "SPARK_MASTER_PORT=7077"          >> spark-env.sh
   echo "SPARK_MASTER_WEBUI_PORT=8080"    >> spark-env.sh
   ```

3. 启动 Master：

   ```
   start-master.sh
   ```

4. 在阿里云控制台 **安全组** 中开放 Master 的 Web UI 和集群端口：

   - 对外（可选）：`8080`（查看 Spark Web UI）
   - 对内网：`7077`（Spark Master RPC）
   - 以及 Worker 节点需要的随机 Shuffle 端口（通常放开同安全组内互访）

5. 在浏览器访问：

   ```
   http://<MASTER 公网 IP>:8080
   ```

   可以看到 Spark Master Web UI，记住页面上的 Master URL，例如：

   ```
   spark://172.24.49.56:7077
   ```

> 后续 Worker 和 `spark-submit` 都要用 **内网 IP 的 Master URL**，即 `spark://172.24.49.56:7077` 这种形式。

------

### 3.2 Worker 节点配置（B 同学、D 同学各自完成）

在 **各自的服务器上**，需要完成以下操作：

1. 安装 JDK 11、下载并解压 Spark（步骤与 Master 类似）

2. 配置 `spark-env.sh`（每台 Worker 自己填核心数 / 内存）：

   ```
   cd ~/spark/conf
   cp spark-env.sh.template spark-env.sh
   
   # Worker 本机资源配置（示例）
   echo "SPARK_WORKER_CORES=2"        >> spark-env.sh   # 按自己机器实际核数调整
   echo "SPARK_WORKER_MEMORY=4g"      >> spark-env.sh   # 按自己内存大小调整
   echo "SPARK_WORKER_PORT=7078"      >> spark-env.sh   # Worker RPC 端口，可保持一致
   ```

3. 启动 Worker，并连接到 Master：

   ```
   # 将 spark://172.24.49.56:7077 换成 A 同学给到的 Master URL
   $SPARK_HOME/sbin/start-worker.sh spark://172.24.49.56:7077
   ```

4. 由 A 同学在 Web UI（`http://<MASTER 公网 IP>:8080`）上确认：

   - 页面下方 **Workers** 区域中，应当能看到 3 个 Worker，分别对应 A / B / D 三台机器。

> 如果 Worker 一直连不上，优先检查：
>
> - 安全组是否允许同一安全组内机器互访 7077 / 7078 / 随机端口；
> - 是否写错了 Master 内网 IP。

------

## 4. 实验工作负载与脚本

### 4.1 数据生成方式与实验场景

当前版本的实验脚本在运行时 **自动生成合成数据集**，不依赖预先落盘的数据。三个实验（Hash / Range / Custom）输入的数据具备完全一致的分布特性，因此变量是被严格控制的。

支持两个典型场景：

1. **均匀分布（`--input-type uniform`）**
   - key 在 `[0, NUM_KEYS)` 上近似均匀分布；
   - value 为字符串 / 数值，用于模拟一定 payload；
   - 适合观察「无明显倾斜」时，不同分区策略对性能的细微影响。
2. **倾斜分布（`--input-type skewed`）**
   - 设置一个或多个热点 key，例如 `0`；
   - 大约 80–90% 的记录集中在热点 key，其余分散在其他 key 上；
   - 模拟真实场景中的 **数据倾斜（data skew）** 问题，便于观察 tail task。

### 4.2 公共命令行参数（各脚本通用）

三个脚本的大部分参数是一致的，典型包括：

- `--input-type {uniform, skewed}`
   数据分布类型。
- `--num-records N`
   生成记录数，例如 `2000000`、`5000000`。
- `--num-partitions P`
   RDD / DataFrame 分区数（即下游分区器的目标分区数）。
- `--print-sample`
   可选，打印少量采样数据，便于检查。
- `--output-dir PATH`
   指定指标输出目录（默认会写入 `code/results/`）。

> 完整参数可以通过：
>
> ```
> python code/jobs/range_basic.py --help
> python code/jobs/hash_basic.py --help
> python code/jobs/custom_basic.py --help
> ```
>
> 来查看。

------

## 5. 运行方式

### 5.1 本地调试（单机）

用于 A / B / C / D 各自在本地快速验证逻辑：

```
# 仅在有 Python 环境的机器上需要
conda activate spark310  # 或其它 Python 3.10 环境
cd ~/spark-partition-strategy

# 例如测试 Range + 均匀数据
bash code/scripts/run_range_basic.sh \
  --master local[*] \
  --input-type uniform \
  --num-records 200000 \
  --num-partitions 16 \
  --print-sample
```

> 说明：
>
> - `--master local[*]` 表示伪分布式，只用于功能调试，**不用于最终实验结果**。

### 5.2 集群模式（3 节点）

最终实验统一由 **A 同学在 Master 节点** 上执行：

```
conda activate spark310
cd ~/spark-partition-strategy
```

#### 5.2.1 RangePartitioner 实验示例

- 均匀数据，2M 记录，16 分区：

```
bash code/scripts/run_range_basic.sh \
  --master spark://172.24.49.56:7077 \
  --input-type uniform \
  --num-records 2000000 \
  --num-partitions 16 \
  --print-sample
```

- 倾斜数据，5M 记录，64 分区：

```
bash code/scripts/run_range_basic.sh \
  --master spark://172.24.49.56:7077 \
  --input-type skewed \
  --num-records 5000000 \
  --num-partitions 64
```

#### 5.2.2 HashPartitioner 实验示例

```
bash code/scripts/run_hash_basic.sh \
  --master spark://172.24.49.56:7077 \
  --input-type uniform \
  --num-records 2000000 \
  --num-partitions 16
bash code/scripts/run_hash_basic.sh \
  --master spark://172.24.49.56:7077 \
  --input-type skewed \
  --num-records 5000000 \
  --num-partitions 64
```

#### 5.2.3 自定义 Partitioner 实验示例

自定义分区器支持对热点 key 做特殊处理（例如把 `key=0` 单独拆到多个分区中，缓解倾斜）：

```
bash code/scripts/run_custom_basic.sh \
  --master spark://172.24.49.56:7077 \
  --input-type uniform \
  --num-records 2000000 \
  --num-partitions 16
bash code/scripts/run_custom_basic.sh \
  --master spark://172.24.49.56:7077 \
  --input-type skewed \
  --num-records 5000000 \
  --num-partitions 64
```

> ✅ 实验完成后，对应的 JSON 指标会自动写入 `code/results/` 目录，文件名中包含：
>
> - 分区策略（hash / range / custom）
> - 数据分布（uniform / skewed）
> - 分区数 / 记录数 / 时间戳等信息。

------

## 6. 指标与实验结果概览（当前轮）

每次实验会记录两类核心指标：

1. **Job 级别耗时**
   - `job_durations_ms`: 各个 Job 的执行时长（毫秒）。
2. **Task 级别时延分布**
   - `task_duration_stats_ms`:
     - `min_ms`, `p50_ms`, `p90_ms`, `p99_ms`, `max_ms`, `count`（task 总数）

当前已完成的代表性实验包括：

- `range_uniform_p16_2m_20251201_194921.json`
- `hash_uniform_p16_2m_20251201_195156.json`
- `custom_uniform_hotnone_b4_p16_2m_20251201_195414.json`
- `range_skewed_p64_5m_20251201_195104.json`
- `hash_skewed_p64_5m_20251201_195309.json`
- `custom_skewed_hot0_b4_p64_5m_20251201_195540.json`

### 6.1 均匀数据（2M 记录，16 分区）

- Hash / Range / Custom 三种分区策略下：
  - Task 的 `p50` 在几百毫秒级；
  - `p99` 大约在 2–2.6 秒；
  - `max` 也都在 2–3 秒区间；
- 总体上三种策略性能 **非常接近**，说明在没有明显倾斜的场景下：
  - 不同分区器在本实验规模下对整体性能影响不大；
  - Partition 数量和集群资源往往比 Partition 类型更关键。

### 6.2 倾斜数据（5M 记录，64 分区）

- 在明显存在热点 key 的 Skew 工作负载下：
  - **HashPartitioner（`hash_skewed_\*`）**
    - `p99` 仍在 2–3 秒左右；
    - 但 `max_ms` 出现了 **约 6 秒以上的 tail task**；
    - 说明某些分区承担了大量热点 key，成为瓶颈。
  - **RangePartitioner（`range_skewed_\*`）**
    - 情况类似，也出现了 **约 9 秒级的 tail task**；
    - 在某些 key 分布下，Range 未必能自然避免倾斜。
  - **自定义 Partitioner（`custom_skewed_\*`）**
    - 通过将热点 `key=0` 拆分到多个分区，或者单独负载均衡；
    - 大部分 task 时延保持在 1 秒以内；
    - `max_ms` 虽然仍有尾巴，但整体 **更接近 p99**，尾部拖累明显减轻；
    - 对应 Job 级别的总运行时间也有一定下降。

------

## 7. 结论与经验

1. **均匀数据场景**
   - Hash / Range / 自定义 分区器在实验规模下性能近似；
   - 更敏感的是：分区数、集群资源（核数 / 内存）以及是否避免过多小文件。
2. **数据倾斜场景**
   - 简单的 Hash / Range 分区在有明显热点 key 时，会造成某些 Task 极慢；
   - 尾部任务（tail task）的存在使得 Job 总耗时被少数分区严重拖累。
3. **自定义 Partitioner 的价值**
   - 针对具体业务的 key 分布（比如热点用户 / 热点标签），合理设计 Partitioner 可以显著缓解 Skew；
   - 实验中，自定义 Partitioner 将热点 key 拆分后：
     - Task 时延分布更均匀；
     - tail task 时长降低，Job 整体完成时间更稳定。

------

## 8. 分工说明

- **同学 A**
  - 搭建与维护 Spark 集群（Master + Worker-1）
  - 协调 B / D 部署各自 Worker
  - 设计数据均匀/倾斜分布场景
  - 统一在 Master 上提交实验作业，收集日志与 JSON 指标
  - 整理并更新本 `README.md` 和基础脚本
- **同学 B**
  - 负责 HashPartitioner 部分实验代码（`code/jobs/hash_*`）
  - 编写并维护 Hash 实验脚本（`code/scripts/run_hash_*.sh`）
  - 在自己的服务器上部署 Worker-2，并接入 Master 集群
- **同学 C**
  - 负责 RangePartitioner 与自定义 Partitioner 代码（`code/jobs/range_*`, `code/jobs/custom_*`）
  - 编写对应运行脚本（`code/scripts/run_range_*.sh`, `code/scripts/run_custom_*.sh`）
  - 负责绘图与可视化分析（`docs/figures/`）
- **同学 D**
  - 在自己的服务器上部署 Worker-3，并接入 Master 集群
  - 撰写实验结果与分析.md
  - 制作实验答辩PPT



> 贡献度：A ≈ C > B ≈ D