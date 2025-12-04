#!/usr/bin/env bash
set -e

# Range 分区实验运行脚本
#
# 用法示例（在 Master 上）：
#   # 自动解析参数 → 数据目录名，规则与 DataGen.py 保持一致：
#   #   <type>_<records_tag>_ratio<skew_ratio>
#   #   例如: type=skewed, num_records=5_000_000, skew_ratio=0.95
#   #        → records_tag=5m
#   #        → 目录名=skewed_5m_ratio0.95
#   #        → 最终 input-path=file:///home/admin/spark-data/skewed_5m_ratio0.95
#   code/scripts/run_range_basic.sh \
#     --data-type skewed \
#     --num-records 5000000 \
#     --skew-ratio 0.95 \
#     --num-partitions 32

cd ~/spark-partition-strategy

export PYTHONPATH="$(pwd)/code:${PYTHONPATH:-}"
MASTER="spark://172.24.49.56:7077"
# MASTER="local[*]"

# 1. 解析数据相关参数，并构造 dataset_name 与 input-path
DATA_ROOT="file:///home/admin/spark-data"

ARGS=("$@")
FORWARDED_ARGS=()
data_type="skewed"      # 默认值，可被 --data-type 覆盖
num_records=5000000      # 默认值，可被 --num-records 覆盖
skew_ratio=0.95          # 默认值，可被 --skew-ratio 覆盖
num_partitions="p?"

_build_records_tag() {
  local n=$1
  if [ $((n % 1000000)) -eq 0 ]; then
    echo "$((n / 1000000))m"
  elif [ $((n % 1000)) -eq 0 ]; then
    echo "$((n / 1000))k"
  else
    echo "$n"
  fi
}

i=0
while [ $i -lt ${#ARGS[@]} ]; do
  arg="${ARGS[$i]}"
  case "$arg" in
    --data-type)
      data_type="${ARGS[$((i+1))]}"
      i=$((i+1))
      ;;
    --num-records)
      num_records="${ARGS[$((i+1))]}"
      i=$((i+1))
      ;;
    --skew-ratio)
      skew_ratio="${ARGS[$((i+1))]}"
      i=$((i+1))
      ;;
    --num-partitions)
      part_val="${ARGS[$((i+1))]}"
      num_partitions="p${part_val}"
      FORWARDED_ARGS+=("--num-partitions" "${part_val}")
      i=$((i+1))
      ;;
    *)
      FORWARDED_ARGS+=("${arg}")
      ;;
  esac
  i=$((i+1))
done

records_tag=$(_build_records_tag "$num_records")
dataset_name="${data_type}_${records_tag}_ratio${skew_ratio}"
input_path="${DATA_ROOT}/${dataset_name}"

echo "[run_range_basic] Using dataset: ${dataset_name}"
echo "[run_range_basic] Resolved input-path: ${input_path}"

# 2. 运行 Range 实验（自动补全 --input-path 与 --input-type）
echo "[run_range_basic] Submitting job to $MASTER..."
spark-submit \
  --master $MASTER \
  code/jobs/range_basic.py \
  --input-path "${input_path}" \
  --input-type "${data_type}" \
  "${FORWARDED_ARGS[@]}"

# 3. 自动解析 Tag
# 结果 JSON 命名为：
#   range_<dataset_name>_p<num_partitions>_timestamp.json
TAG="${dataset_name}_${num_partitions}"

echo "[run_range_basic] Collecting latest event log with tag: ${TAG}"

python3 code/tools/collect_latest_eventlog.py \
  range \
  --results-dir code/results \
  --tag "${TAG}"
