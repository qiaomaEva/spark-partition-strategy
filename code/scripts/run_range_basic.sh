#!/usr/bin/env bash
set -e

# Range 分区实验运行脚本（同学 C）
#
# 用法示例（在 Master 上）：
#   code/scripts/run_range_basic.sh \
#     --input-type skewed \
#     --num-records 10000000 \
#     --num-partitions 128
#
# 本脚本会：
#   1）使用 spark-submit 运行 code/jobs/range_basic.py；
#   2）在 logs/spark-events/ 下找到最新的 event log；
#   3）调用 code/tools/collect_latest_eventlog.py + parse_spark_eventlog.py，
#      自动生成一个 JSON 到 code/results/，文件名前缀为 range_...。

# Master 上用 conda 管理 Spark 环境时需要激活环境，本地测试可注释掉
# conda activate spark310

cd ~/spark-partition-strategy

# 1. 运行 Range 实验
# 本地测试时可以改为 --master local[*]
spark-submit \
  --master spark://172.23.166.104:7078 \
  code/jobs/range_basic.py \
  "$@"

# 2. 实验结束后，自动解析最新 event log 并导出 JSON
#  ARGS 解析，生成 TAG
# 可以根据需要在 TAG 里写一个简短描述（可选）
TAG=""

# 从参数里简单推导一个 tag（非必须，只是让文件名更易懂）
ARGS=("$@")
input_type="unknown"
num_partitions="p?"
num_records_tag="n?"

i=0
while [ $i -lt ${#ARGS[@]} ]; do
  arg="${ARGS[$i]}"
  case "$arg" in
    --input-type)
      input_type="${ARGS[$((i+1))]}"
      i=$((i+1))
      ;;
    --num-partitions)
      num_partitions="p${ARGS[$((i+1))]}"
      i=$((i+1))
      ;;
    --num-records)
      nr="${ARGS[$((i+1))]}"
      if [ "$nr" -ge 1000000 ]; then
        num_records_tag="$((nr / 1000000))m"
      elif [ "$nr" -ge 1000 ]; then
        num_records_tag="$((nr / 1000))k"
      else
        num_records_tag="${nr}"
      fi
      i=$((i+1))
      ;;
  esac
  i=$((i+1))
done

TAG="${input_type}_${num_partitions}_${num_records_tag}"

echo "[run_range_basic] Collecting latest event log with tag: ${TAG}"

# 3. 调用 collect_latest_eventlog.py 脚本
python3 code/tools/collect_latest_eventlog.py \
  range \
  --event-dir logs/spark-events \
  --results-dir code/results \
  --tag "${TAG}"