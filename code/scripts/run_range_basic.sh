#!/usr/bin/env bash
set -e

# Range 分区实验运行脚本
#
# 用法示例（在 Master 上）：
#   code/scripts/run_range_basic.sh \
#     --input-type skewed \
#     --input-path file:///home/admin/spark-data/skewed_5m_ratio0.95 \
#     --num-partitions 32

cd ~/spark-partition-strategy

export PYTHONPATH="$(pwd)/code:${PYTHONPATH:-}"
MASTER="spark://172.24.49.56:7077"

# 1. 运行 Range 实验
echo "[run_range_basic] Submitting job to $MASTER..."
spark-submit \
<<<<<<< HEAD
  --master local[*] \
=======
  --master $MASTER \
>>>>>>> f2d8558 (使用datagen生成数据集保存至各个节点，并修改code中对应部分代码)
  code/jobs/range_basic.py \
  "$@"

# 2. 自动解析 Tag
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

TAG="range_${dataset_name}_${num_partitions}"

echo "[run_range_basic] Collecting latest event log with tag: ${TAG}"

python3 code/tools/collect_latest_eventlog.py \
  range \
  --event-dir logs/spark-events \
  --results-dir code/results \
  --tag "${TAG}"