#!/bin/bash

# ============================================================
# Spark 分区策略对比实验 - 全流程自动化脚本
# ============================================================

# 遇到错误立即停止，避免后续实验在错误数据上运行
set -e

# 确保在项目根目录
cd ~/spark-partition-strategy

# 设置 Python 路径，确保找不到模块时不报错
export PYTHONPATH="$(pwd)/code:${PYTHONPATH:-}"

echo "========================================================"
echo "STARTING FULL EXPERIMENT SUITE"
echo "Start Time: $(date)"
echo "========================================================"

# ------------------------------------------------------------
# 1. 均匀场景 (Uniform, 2m, p16)
# ------------------------------------------------------------
echo ""
echo "[Step 1/3] Running Uniform Scenario (2m records, 16 partitions)..."


# 运行实验
echo "  -> Running Hash..."
code/scripts/run_hash_basic.sh --data-type uniform --num-records 2000000 --skew-ratio 0.0 --num-partitions 16

echo "  -> Running Range..."
code/scripts/run_range_basic.sh --data-type uniform --num-records 2000000 --skew-ratio 0.0 --num-partitions 16

echo "  -> Running Custom (b1)..."
code/scripts/run_custom_basic.sh --data-type uniform --num-records 2000000 --skew-ratio 0.0 --num-partitions 16 --hot-keys 0 --hot-bucket-factor 1


# ------------------------------------------------------------
# 2. 温和倾斜场景 (Skew 0.75, 5m, p64)
# ------------------------------------------------------------
echo ""
echo "[Step 2/3] Running Mild Skew Scenario (0.75 ratio, 5m records, 64 partitions)..."

#  运行实验
echo "  -> Running Hash..."
code/scripts/run_hash_basic.sh --data-type skewed --num-records 5000000 --skew-ratio 0.75 --num-partitions 64

echo "  -> Running Range..."
code/scripts/run_range_basic.sh --data-type skewed --num-records 5000000 --skew-ratio 0.75 --num-partitions 64

echo "  -> Running Custom (b4)..."
code/scripts/run_custom_basic.sh --data-type skewed --num-records 5000000 --skew-ratio 0.75 --num-partitions 64 --hot-keys 0 --hot-bucket-factor 4

echo "  -> Running Custom (b8)..."
code/scripts/run_custom_basic.sh --data-type skewed --num-records 5000000 --skew-ratio 0.75 --num-partitions 64 --hot-keys 0 --hot-bucket-factor 8

echo "  -> Running Custom (b32)..."
code/scripts/run_custom_basic.sh --data-type skewed --num-records 5000000 --skew-ratio 0.75 --num-partitions 64 --hot-keys 0 --hot-bucket-factor 32


# ------------------------------------------------------------
# 3. 极端倾斜场景 (Skew 0.95, 5m, p64)
# ------------------------------------------------------------
echo ""
echo "[Step 3/3] Running Extreme Skew Scenario (0.95 ratio, 5m records, 64 partitions)..."

# 运行实验
echo "  -> Running Hash..."
code/scripts/run_hash_basic.sh --data-type skewed --num-records 5000000 --skew-ratio 0.95 --num-partitions 64

echo "  -> Running Range..."
code/scripts/run_range_basic.sh --data-type skewed --num-records 5000000 --skew-ratio 0.95 --num-partitions 64

echo "  -> Running Custom (b4)..."
code/scripts/run_custom_basic.sh --data-type skewed --num-records 5000000 --skew-ratio 0.95 --num-partitions 64 --hot-keys 0 --hot-bucket-factor 4

echo "  -> Running Custom (b8)..."
code/scripts/run_custom_basic.sh --data-type skewed --num-records 5000000 --skew-ratio 0.95 --num-partitions 64 --hot-keys 0 --hot-bucket-factor 8

echo "  -> Running Custom (b32)..."
code/scripts/run_custom_basic.sh --data-type skewed --num-records 5000000 --skew-ratio 0.95 --num-partitions 64 --hot-keys 0 --hot-bucket-factor 32


# ------------------------------------------------------------
# 4. 汇总与画图
# ------------------------------------------------------------
echo ""
echo "========================================================"
echo "Generating Reports..."
python3 code/tools/summarize_results.py
python3 code/tools/plot_summary.py

echo "========================================================"
echo "ALL DONE!"
echo "End Time: $(date)"
echo "Check code/results/ for CSV and PNG files."
