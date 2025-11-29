## 如何从一次实验跑到结果 JSON（含路径设计）

1. **跑实验（Range 脚本）**  
   例：在 WSL 里：

   ```bash
   cd ~/spark-partition-strategy

   spark-submit \
     --master local[*] \
     code/jobs/range_basic.py \
     --input-type skewed \
     --input-path data \
     --num-partitions 4
   ```

   这一步会在 `logs/spark-events/` 写出一个 event log 文件，比如：

   ```text
   logs/spark-events/application_....json
   ```

   你可以 `ls logs/spark-events` 看一下文件名。

2. **用解析脚本生成结果 JSON 到 `code/results/`**

   假设刚产生了一个日志文件：

   ```bash
   ls logs/spark-events
   # 得到一个文件名，比如 application_1234567890000_0001
   ```

   手动跑一次解析（后面可以写 shell 自动化）：

   ```bash
   python3 code/tools/parse_spark_eventlog.py \
     logs/spark-events/application_1234567890000_0001 \
     code/results/range_skewed_p4_20251127.json
   ```

   这会在 `code/results/` 下生成一个 JSON 文件：

   ```json
   {
     "job_count": ...,
     "stage_count": ...,
     "task_count": ...,
     "job_durations_ms": [...],
     "stage_durations_ms": [...],
     "total_shuffle_read_bytes": ...,
     "total_shuffle_write_bytes": ...,
     "task_duration_stats_ms": {
       "min_ms": ...,
       "p50_ms": ...,
       "p90_ms": ...,
       "p99_ms": ...,
       "max_ms": ...,
       "count": ...
     }
   }
   ```

   这个就完全符合 README 里“记录与分析指标，并导出到 `code/results/`”的要求了，也很方便 D 同学后处理。