#!/usr/bin/env bash
set -e

# Master 上是用 conda 管理 Spark 环境，需要激活环境
# 本地测试时注释掉即可
# conda activate spark310

cd ~/spark-partition-strategy

# 本地测试： --master local[*] \
# master测试： --master spark://172.23.166.104:7078 \
spark-submit \
  --master spark://172.23.166.104:7078 \
  code/jobs/custom_basic.py \
  "$@"