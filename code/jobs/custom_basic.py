import os
from datetime import datetime
import argparse
import time
from typing import Dict, Any, Set

from pyspark.sql import SparkSession
from pyspark.rdd import RDD
from pyspark import SparkConf

from data_gen.DataGen import HOT_KEY

from jobs.common import (
    load_rdd_from_parquet,
    compute_partition_distribution,
)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Custom skew-aware partitioner experiment (by C)."
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
        help="number of partitions (logical partitions).",
    )

    parser.add_argument(
        "--hot-keys",
        type=str,
        default="",
        help='comma-separated list of hot keys, e.g. "0,1,2". '
             "Empty string means no predefined hot keys.",
    )

    parser.add_argument(
        "--hot-bucket-factor",
        type=int,
        default=4,
        help="number of sub-buckets for each hot key (>=1).",
    )

    parser.add_argument(
        "--print-sample",
        action="store_true",
        help="print sample of (key, sum) result for debugging.",
    )

    return parser.parse_args()


# ---------- 自定义 skew-aware 分区逻辑 (保持不变) ----------

def parse_hot_keys(hot_keys_str: str) -> Set[int]:
    if not hot_keys_str.strip():
        return set()
    parts = [s.strip() for s in hot_keys_str.split(",") if s.strip() != ""]
    return {int(x) for x in parts}


def custom_partition_func(num_partitions: int, hot_keys: Set[int], hot_bucket_factor: int):
    if hot_bucket_factor < 1:
        hot_bucket_factor = 1

    def partitioner(wrapped_key):
        orig_key, sub_bucket = wrapped_key
        if orig_key in hot_keys:
            if len(hot_keys) == 1 and hot_bucket_factor >= num_partitions:
                return sub_bucket % num_partitions
            base = hash(orig_key) % num_partitions
            offset = sub_bucket % hot_bucket_factor
            return (base + offset) % num_partitions
        else:
            return hash(orig_key) % num_partitions
    return partitioner


def wrap_keys_for_custom_partitioner(rdd: RDD, hot_keys: Set[int], hot_bucket_factor: int) -> RDD:
    if hot_bucket_factor < 1:
        hot_bucket_factor = 1

    def wrap_pair(kv):
        k, v = kv
        if k in hot_keys:
            sub_bucket = hash(v) % hot_bucket_factor
        else:
            sub_bucket = 0
        return ( (k, sub_bucket), v )

    return rdd.map(wrap_pair)


def unwrap_keys_after_agg(rdd: RDD) -> RDD:
    return rdd.map(lambda kv: (kv[0][0], kv[1])).reduceByKey(lambda x, y: x + y)


def run_custom_experiment(
    rdd: RDD,
    num_partitions: int,
    hot_keys: Set[int],
    hot_bucket_factor: int,
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    metrics["partition_distribution_before"] = compute_partition_distribution(rdd)

    # 包装 key
    wrapped_rdd = wrap_keys_for_custom_partitioner(rdd, hot_keys, hot_bucket_factor)
    partitioner = custom_partition_func(num_partitions, hot_keys, hot_bucket_factor)

    t0 = time.time()
    partitioned_rdd = wrapped_rdd.partitionBy(num_partitions, partitionFunc=partitioner)
    
    # 强制计算 partition 分布，计入 Shuffle 开销
    t_dist_start = time.time()
    metrics["partition_distribution_after_custom"] = compute_partition_distribution(partitioned_rdd)
    t_dist_end = time.time()
    metrics["t_partition"] = (t_dist_end - t0) # 近似包含 shuffle write/read

    # 聚合
    t2 = time.time()
    aggregated_rdd = partitioned_rdd.reduceByKey(lambda x, y: x + y)
    aggregated_rdd.count()
    t3 = time.time()
    metrics["t_agg"] = t3 - t2
    metrics["t_total"] = (metrics["t_partition"]) + (t3 - t2)

    # 聚合后分区分布（注意这里的 key 还是 (key, sub_bucket)）
    metrics["partition_distribution_after_reduce_raw"] = compute_partition_distribution(aggregated_rdd)

    # 第二轮：按原始 key 还原并再次聚合
    final_rdd = unwrap_keys_after_agg(aggregated_rdd)

    # 最终结果的分区分布（按原始 key）
    metrics["partition_distribution_final"] = compute_partition_distribution(final_rdd)

    # 暴露最终结果 RDD，方便上层采样或保存
    metrics["result_rdd"] = final_rdd
    return metrics


def main():
    args = parse_args()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    event_log_dir = os.path.join(project_root, "logs", "spark-events")
    os.makedirs(event_log_dir, exist_ok=True)

    conf = (
        SparkConf()
        .setAppName(f"CustomPartitionerBasic-{args.input_type}-{run_id}")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", f"file://{event_log_dir}")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    print("===== Custom Partitioner Basic Experiment (by C) =====")
    print(f"Input path     : {args.input_path}")
    print(f"Num partitions : {args.num_partitions}")

    # 加载数据
    rdd = load_rdd_from_parquet(spark, args.input_path, args.num_partitions)

    hot_keys = parse_hot_keys(args.hot_keys)
    # 如果是倾斜数据且未指定热点key，默认使用 HOT_KEY (0)
    if not hot_keys and args.input_type == "skewed":
        hot_keys = {HOT_KEY}

    metrics = run_custom_experiment(
        rdd,
        num_partitions=args.num_partitions,
        hot_keys=hot_keys,
        hot_bucket_factor=args.hot_bucket_factor,
    )
    result_rdd = metrics.pop("result_rdd")

    if args.print_sample:
        result_sample = sorted(result_rdd.collect())[:20]
        print("Result sample:", result_sample)

    print("----- Metrics (Custom Partitioner) -----")
    print(f"t_partition_seconds      : {metrics['t_partition']:.6f}")
    print(f"t_agg_seconds            : {metrics['t_agg']:.6f}")
    print(f"t_total_seconds_approx   : {metrics['t_total']:.6f}")
    
    print("partition_distribution_after_custom (pid -> count):")
    for pid, cnt in metrics["partition_distribution_after_custom"]:
        print(f"  {pid} -> {cnt}")

    spark.stop()


if __name__ == "__main__":
    main()