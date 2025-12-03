#!/usr/bin/env bash
set -e

# Custom 分区实验运行脚本
#
# 用法示例（在 Master 上）：
#   code/scripts/run_custom_basic.sh \
#     --input-type skewed \
#     --input-path file:///home/admin/spark-data/skewed_5m_ratio0.95 \
#     --num-partitions 32 \
#     --hot-keys "0" \
#     --hot-bucket-factor 32

cd ~/spark-partition-strategy

export PYTHONPATH="$(pwd)/code:${PYTHONPATH:-}"
MASTER="spark://172.24.49.56:7077"

# 1. 运行 Custom 实验
echo "[run_custom_basic] Submitting job to $MASTER..."
spark-submit \
<<<<<<< HEAD
  --master local[*] \
=======
  --master $MASTER \
>>>>>>> f2d8558 (使用datagen生成数据集保存至各个节点，并修改code中对应部分代码)
  code/jobs/custom_basic.py \
  "$@"

# 2. 自动解析 Tag
TAG="{input_type}_${hot_keys_tag}_${bucket_tag}_${num_partitions}_${dataset_name}"
ARGS=("$@")
dataset_name="unknown"
num_partitions="p?"
bucket_tag="b?"
hot_keys_tag="hotnone"

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
    --hot-keys)
      hk="${ARGS[$((i+1))]}"
      if [ -z "$hk" ]; then
        hot_keys_tag="hotnone"
      else
        # 简单处理热点 key 字符串，避免文件名非法字符
        hot_keys_tag="hot${hk//,/}"
      fi
      i=$((i+1))
      ;;
    --hot-bucket-factor)
      bucket_tag="b${ARGS[$((i+1))]}"
      i=$((i+1))
      ;;
  esac
  i=$((i+1))
done

# 生成类似: custom_skewed_5m_ratio0.95_hot0_b32_p32
TAG="custom_${dataset_name}_${hot_keys_tag}_${bucket_tag}_${num_partitions}"

echo "[run_custom_basic] Collecting latest event log with tag: ${TAG}"

python3 code/tools/collect_latest_eventlog.py \
  custom \
  --tag "${TAG}"