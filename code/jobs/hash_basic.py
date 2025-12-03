import os
from datetime import datetime
import argparse
import time
from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.rdd import RDD
from pyspark import SparkConf

from jobs.common import (
    load_rdd_from_parquet,
    compute_partition_distribution,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Basic Hash Partitioning experiment (by B)."
    )

    parser.add_argument(
        "--input-path",
        type=str,
        required=True,
        help="Path to the input parquet data (must exist on all nodes if local file)."
    )
    
    parser.add_argument(
        "--input-type",
        type=str,
        default="unknown",
        help="Label for input type (e.g. uniform/skewed) for logging purposes."
    )

    parser.add_argument(
        "--num-partitions",
        type=int,
        default=4,
        help="Target number of partitions for the RDD / partitionBy.",
    )

    parser.add_argument(
        "--print-sample",
        action="store_true",
        help="print sample of (key, sum) result for debugging.",
    )

    args = parser.parse_args()
    return args


# ---------- 核心实验逻辑 (Hash Partitioner) ----------

def run_hash_experiment(rdd: RDD, num_partitions: int) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    # 1. PartitionBy 之前的分区分布
    metrics["partition_distribution_before"] = compute_partition_distribution(rdd)

    # 2. PartitionBy (Hash)
    t0 = time.time()
    partitioned_rdd: RDD = rdd.partitionBy(numPartitions=num_partitions)
    # 强制执行以测量 partitionBy (Shuffle Write) 的开销
    # 注意：rdd.partitionBy 是 Lazy 的，但为了单独测量 Shuffle 阶段，通常需要跟一个 Action
    # 或者我们在后续聚合中测量总时间。为了保持和原来逻辑一致，这里不单独触发 Action，
    # 而是依赖后续的 reduceByKey 来触发。
    # *修正*: 为了明确看到 partition 的效果，通常我们会在这之后查看分区分布，这会触发 Shuffle。
    t1 = time.time()
    metrics["t_partition_define"] = t1 - t0 # 只是定义的时间，几乎为0

    # 3. PartitionBy 后的分区分布 (这会触发 Shuffle)
    t_dist_start = time.time()
    metrics["partition_distribution_after_hash"] = compute_partition_distribution(partitioned_rdd)
    t_dist_end = time.time()
    metrics["t_partition_shuffle"] = t_dist_end - t_dist_start

    # 4. 第一次 reduceByKey
    # 由于数据已经按 Hash 分区，理论上 reduceByKey 这里的 Shuffle 开销会很小（主要是 map-side combine 后直接聚合），
    # 除非 Spark 优化器决定重新 Shuffle。
    t2 = time.time()
    reduced_rdd = partitioned_rdd.reduceByKey(lambda x, y: x + y)
    reduced_rdd.count()  # 触发第一次聚合的 Action

    t3 = time.time()
    
    metrics["t_agg"] = t3 - t2
    metrics["t_total"] = (t_dist_end - t_dist_start) + (t3 - t2)

    # 5. 第一次 reduce 后的分区分布
    metrics["partition_distribution_after_reduce"] = compute_partition_distribution(reduced_rdd)

    # 6. 第二次 reduceByKey（形式上与 custom/range 的第二轮聚合对齐）
    # 对于 Hash 实验来说，key 本身没有变化，这一轮在语义上是幂等的，但会在 DAG 中体现为额外的 Stage。
    final_rdd = reduced_rdd.reduceByKey(lambda x, y: x + y)

    # 7. 最终结果的分区分布
    metrics["partition_distribution_final"] = compute_partition_distribution(final_rdd)

    # 8. 返回最终结果 RDD
    metrics["result_rdd"] = final_rdd

    return metrics


def main():
    args = parse_args()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    event_log_dir = os.path.join(project_root, "logs", "spark-events")
    os.makedirs(event_log_dir, exist_ok=True)

    # 使用 SparkSession (推荐)
    conf = (
        SparkConf()
        .setAppName(f"HashPartitionerBasic-{args.input_type}-{run_id}")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", f"file://{event_log_dir}")
    )
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext

    print("===== HashPartitioner Basic Experiment (by B) =====")
    print(f"Input path     : {args.input_path}")
    print(f"Input type (lbl): {args.input_type}")
    print(f"Num partitions : {args.num_partitions}")

    # 1. 加载数据
    # 注意：load_rdd_from_parquet 会处理读取和 repartition
    rdd = load_rdd_from_parquet(spark, args.input_path, args.num_partitions)

    # 2. 运行实验
    metrics = run_hash_experiment(rdd, args.num_partitions)
    result_rdd: RDD = metrics.pop("result_rdd")

    # 3. 结果展示
    if args.print_sample:
        result_sample = sorted(result_rdd.collect())[:20]
        print("Result sample:", result_sample)

    print("----- Metrics (Hash Partitioner) -----")
    print(f"t_partition_shuffle (s)  : {metrics['t_partition_shuffle']:.6f}")
    print(f"t_agg_seconds            : {metrics['t_agg']:.6f}")
    
    print("Partition dist before:")
    for pid, cnt in metrics["partition_distribution_before"]:
        print(f"  {pid}: {cnt}")

    print("Partition dist after hash:")
    for pid, cnt in metrics["partition_distribution_after_hash"]:
        print(f"  {pid}: {cnt}")

    spark.stop()

if __name__ == "__main__":
    main()