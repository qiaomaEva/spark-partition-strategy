#!/usr/bin/env bash
set -e

# Hash 分区实验运行脚本
#
# 用法示例（在 Master 上）：
#   code/scripts/run_hash_basic.sh \
#     --input-type skewed \
#     --input-path file:///home/admin/spark-data/skewed_5m_ratio0.95 \
#     --num-partitions 32

cd ~/spark-partition-strategy

export PYTHONPATH="$(pwd)/code:${PYTHONPATH:-}"
MASTER="spark://172.24.49.56:7077"

# 1. 运行 Hash 实验
echo "[run_hash_basic] Submitting job to $MASTER..."
spark-submit \
  --master $MASTER \
  code/jobs/hash_basic.py \
  "$@"

# 2. 自动解析 Tag 用于生成文件名
# 新逻辑：直接从 --input-path 中提取数据集名称
TAG="${input_type}_${num_partitions}_${dataset_name}"
ARGS=("$@")
dataset_name="unknown"
num_partitions="p?"

i=0
while [ $i -lt ${#ARGS[@]} ]; do
  arg="${ARGS[$i]}"
  case "$arg" in
    --input-path)
      path_val="${ARGS[$((i+1))]}"
      # 提取路径最后一部分作为数据集名称 (例如 uniform_1m 或 skewed_5m)
      dataset_name=$(basename "$path_val")
      i=$((i+1))
      ;;
    --num-partitions)
      num_partitions="p${ARGS[$((i+1))]}"
      i=$((i+1))
      ;;
  esac
  i=$((i+1))
done

TAG="hash_${dataset_name}_${num_partitions}"

echo "[run_hash_basic] Collecting latest event log with tag: ${TAG}"

# 3. 解析 EventLog
python3 code/tools/collect_latest_eventlog.py \
  hash \
  --tag "${TAG}"
