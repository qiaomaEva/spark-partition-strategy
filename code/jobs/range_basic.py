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
        description="Basic Range-like partitioning experiment (by C)."
    )

    parser.add_argument(
        "--input-path",
        type=str,
        required=True,
        help="Path to the input parquet data."
    )
    
    parser.add_argument(
        "--input-type",
        type=str,
        default="unknown",
        help="Label for logging."
    )

    parser.add_argument(
        "--num-partitions",
        type=int,
        default=4,
        help="Number of partitions for sortByKey.",
    )

    parser.add_argument(
        "--print-sample",
        action="store_true",
        help="print sample result."
    )

    args = parser.parse_args()
    return args


def run_range_experiment(rdd: RDD, num_partitions: int) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    metrics["partition_distribution_before"] = compute_partition_distribution(rdd)

    # 2. sortByKey (Range Partitioning happens here)
    t0 = time.time()
    # sortByKey 触发 Shuffle (RangePartitioner)
    sorted_rdd: RDD = rdd.sortByKey(numPartitions=num_partitions)
    # 强制执行以包含 shuffle 时间
    sorted_rdd.count() 
    t1 = time.time()
    metrics["t_sort"] = t1 - t0

    metrics["partition_distribution_after_sort"] = compute_partition_distribution(sorted_rdd)

    # 4. reduceByKey
    t2 = time.time()
    reduced_rdd: RDD = sorted_rdd.reduceByKey(lambda x, y: x + y)
    reduced_rdd.count()
    t3 = time.time()
    metrics["t_agg"] = t3 - t2
    metrics["t_total"] = (t1 - t0) + (t3 - t2)

    metrics["partition_distribution_after_reduce"] = compute_partition_distribution(reduced_rdd)
    metrics["result_rdd"] = reduced_rdd

    return metrics


def main():
    args = parse_args()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    event_log_dir = os.path.join(project_root, "logs", "spark-events")
    os.makedirs(event_log_dir, exist_ok=True)

    conf = (
        SparkConf()
        .setAppName(f"RangePartitionerBasic-{args.input_type}-{run_id}")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", f"file://{event_log_dir}")
    )
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    print("===== RangePartitioner Basic Experiment (by C) =====")
    print(f"Input path     : {args.input_path}")
    print(f"Num partitions : {args.num_partitions}")

    # 加载数据
    rdd = load_rdd_from_parquet(spark, args.input_path, args.num_partitions)

    metrics = run_range_experiment(rdd, args.num_partitions)
    result_rdd: RDD = metrics.pop("result_rdd")

    if args.print_sample:
        result_sample = sorted(result_rdd.collect())[:20]
        print("Result sample:", result_sample)

    print("----- Metrics -----")
    print(f"t_sort_seconds           : {metrics['t_sort']:.6f}")
    print(f"t_agg_seconds            : {metrics['t_agg']:.6f}")
    print(f"t_total_seconds_approx   : {metrics['t_total']:.6f}")

    print("partition_distribution_after_sort (pid -> count):")
    for pid, cnt in metrics["partition_distribution_after_sort"]:
        print(f"  {pid} -> {cnt}")

    spark.stop()

if __name__ == "__main__":
    main()