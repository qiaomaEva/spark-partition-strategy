#!/usr/bin/env bash
set -e

# Custom 分区实验运行脚本（同学 C）
#
# 用法示例（在 Master 上）：
#   code/scripts/run_custom_basic.sh \
#     --input-type skewed \
#     --num-records 10000000 \
#     --num-partitions 128 \
#     --hot-keys "0" \
#     --hot-bucket-factor 4
#
# 本脚本会：
#   1）使用 spark-submit 运行 code/jobs/custom_basic.py；
#   2）在 logs/spark-events/ 下找到最新的 event log；
#   3）调用 code/tools/collect_latest_eventlog.py + parse_spark_eventlog.py，
#      自动生成一个 JSON 到 code/results/，文件名前缀为 custom_...。

# Master 上用 conda 管理 Spark 环境时需要激活环境，本地测试可注释掉

cd ~/spark-partition-strategy

# Ensure Python can import the local `jobs` package (code/ contains jobs/)
# Add the `code` directory to PYTHONPATH so `import jobs.*` works.
export PYTHONPATH="$(pwd)/code:${PYTHONPATH:-}"

# 1. 运行 Custom 实验
# 本地测试时可以改为 --master local[*]
spark-submit \
  --master local[*] \
  code/jobs/custom_basic.py \
  "$@"

# 2. 实验结束后，自动解析最新 event log 并导出 JSON
#  ARGS 解析，生成 TAG
# 可以根据需要在 TAG 里写一个简短描述（可选）
TAG=""

ARGS=("$@")
input_type="unknown"
num_partitions="p?"
num_records_tag="n?"
hot_keys_tag="hotnone"
bucket_tag="b?"

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
    --hot-keys)
      hk="${ARGS[$((i+1))]}"
      if [ -z "$hk" ]; then
        hot_keys_tag="hotnone"
      else
        # 把逗号替换成下划线，避免文件名里有逗号或空格
        hot_keys_tag="hot${hk//,/ _}"
        hot_keys_tag="${hot_keys_tag// /}"
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

TAG="${input_type}_${hot_keys_tag}_${bucket_tag}_${num_partitions}_${num_records_tag}"

echo "[run_custom_basic] Collecting latest event log with tag: ${TAG}"

python3 code/tools/collect_latest_eventlog.py \
  custom \
  --tag "${TAG}"
