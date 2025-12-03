import os
from datetime import datetime
import argparse
import time
from typing import Dict, Any, List, Tuple, Set

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD

from jobs.common import (
    NUM_KEYS,
    SKEW_RATIO,
    HOT_KEY,
    build_synthetic_rdd,
    build_uniform_rdd_generated,
    build_skewed_rdd_generated,
    compute_partition_distribution,
)

# ---------- 参数解析 ----------

def parse_args() -> argparse.Namespace:
    """
    自定义 Partitioner 实验参数：
      --input-type: synthetic / uniform / skewed（数据生成模式）
      --num-records: 生成多少条记录
      --num-partitions: 逻辑分区数
      --hot-keys: 用逗号分隔的热点 key 列表（例如: "0,1,2"）
      --hot-bucket-factor: 每个热点 key 拆成多少个子桶位
      --print-sample: 是否打印结果样本（仅本地调试用）
    """
    parser = argparse.ArgumentParser(
        description="Custom skew-aware partitioner experiment (by C)."
    )

    parser.add_argument(
        "--input-type",
        type=str,
        default="synthetic",
        choices=["synthetic", "uniform", "skewed"],
        help=(
            "input data type: "
            "synthetic (simple pattern generated), "
            "uniform (generated in Spark), "
            "skewed (generated in Spark)."
        ),
    )

    parser.add_argument(
        "--num-records",
        type=int,
        default=100000,
        help="number of records to generate for all input types.",
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


# ---------- 自定义 skew-aware 分区逻辑 ----------

def parse_hot_keys(hot_keys_str: str) -> Set[int]:
    if not hot_keys_str.strip():
        return set()
    parts = [s.strip() for s in hot_keys_str.split(",") if s.strip() != ""]
    return {int(x) for x in parts}


def custom_partition_func(num_partitions: int, hot_keys: Set[int], hot_bucket_factor: int):
    """
    返回一个可传给 partitionBy 的分区函数：
      partition_id = f((orig_key, sub_bucket))
    设计：
      - 非热点 key: 正常 hash 到 [0, num_partitions)
      - 热点 key:   先根据 sub_bucket 扩散到多个分区，再 mod 回 [0, num_partitions)
    """
    if hot_bucket_factor < 1:
        hot_bucket_factor = 1

    def partitioner(wrapped_key):
        """
        wrapped_key: (orig_key, sub_bucket)
        """
        orig_key, sub_bucket = wrapped_key
        if orig_key in hot_keys:
            # 对单热点 key 且桶数足够多的情况，更激进地把流量摊到所有分区：
            # 直接使用 sub_bucket 在 [0, num_partitions) 上取模。
            # 这样 95%+ 的热点流量可以几乎平均打到所有分区上。
            if len(hot_keys) == 1 and hot_bucket_factor >= num_partitions:
                return sub_bucket % num_partitions
            # 通用情况：在原始 hash 分区附近做局部扩散
            base = hash(orig_key) % num_partitions
            offset = sub_bucket % hot_bucket_factor
            return (base + offset) % num_partitions
        else:
            # 非热点 key 走普通 hash
            return hash(orig_key) % num_partitions

    return partitioner


def wrap_keys_for_custom_partitioner(
    rdd: RDD,
    hot_keys: Set[int],
    hot_bucket_factor: int,
) -> RDD:
    """
    输入: (key, value)
    输出: ((key, sub_bucket), value)
    对于热点 key，sub_bucket = hash(value) % hot_bucket_factor
    对于非热点 key，sub_bucket 固定为 0
    """
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
    """
    输入: ((key, sub_bucket), agg_value)
    输出: (key, agg_value)
    汇总后的阶段需要按原始 key 再聚合一次（因为同一个 key 可能有多个 sub_bucket）
    """
    return rdd.map(lambda kv: (kv[0][0], kv[1])).reduceByKey(lambda x, y: x + y)


def run_custom_experiment(
    sc: SparkContext,
    rdd: RDD,
    num_partitions: int,
    hot_keys: Set[int],
    hot_bucket_factor: int,
) -> Dict[str, Any]:
    """
    执行自定义 skew-aware 分区实验，返回指标：
      - t_partition: partitionBy 阶段耗时
      - t_agg: reduceByKey 阶段耗时
      - t_total
      - partition_distribution_before: 原始 RDD 分布
      - partition_distribution_after_custom: 自定义分区后分布
      - partition_distribution_after_reduce: 聚合后分布
      - result_rdd: 最终结果 RDD（可选 collect）
    """
    metrics: Dict[str, Any] = {}

    # 原始 RDD 分布
    metrics["partition_distribution_before"] = compute_partition_distribution(rdd)

    # 包装 key，让热点 key 拆桶
    wrapped_rdd = wrap_keys_for_custom_partitioner(rdd, hot_keys, hot_bucket_factor)

    # 自定义分区
    partitioner = custom_partition_func(num_partitions, hot_keys, hot_bucket_factor)

    t0 = time.time()
    partitioned_rdd = wrapped_rdd.partitionBy(num_partitions, partitionFunc=partitioner)
    t1 = time.time()
    metrics["t_partition"] = t1 - t0

    metrics["partition_distribution_after_custom"] = compute_partition_distribution(partitioned_rdd)

    # 聚合
    t2 = time.time()
    aggregated_rdd = partitioned_rdd.reduceByKey(lambda x, y: x + y)
    t3 = time.time()
    metrics["t_agg"] = t3 - t2
    metrics["t_total"] = (t1 - t0) + (t3 - t2)

    # 聚合后分区分布（注意这里的 key 还是 (key, sub_bucket)）
    metrics["partition_distribution_after_reduce_raw"] = compute_partition_distribution(aggregated_rdd)

    # 为了和 hash 实验的算子链更可比，这里先不做第二轮 unwrap+reduce，
    # 直接把 aggregated_rdd 当作最终结果。
    metrics["partition_distribution_final"] = metrics["partition_distribution_after_reduce_raw"]
    metrics["result_rdd"] = aggregated_rdd
    return metrics


# ---------- main ----------

def main():
    args = parse_args()

    # 为当前实验 run 生成一个唯一 ID（方便多次运行区分）
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Spark 事件日志目录（保存到项目根目录下的 logs/spark-events）
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    event_log_dir = os.path.join(project_root, "logs", "spark-events")

    conf = (
        SparkConf()
        .setAppName(f"CustomPartitionerBasic-{run_id}")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", f"file://{event_log_dir}")
    )

    os.makedirs(event_log_dir, exist_ok=True)
    sc = SparkContext.getOrCreate(conf)

    print("===== Custom Partitioner Basic Experiment (by C) =====")
    print(f"Input type        : {args.input_type}")
    print(f"Num records       : {args.num_records}")
    print(f"Num partitions    : {args.num_partitions}")
    print(f"Hot keys          : {args.hot_keys}")
    print(f"Hot bucket factor : {args.hot_bucket_factor}")
    print(f"Print sample      : {args.print_sample}")

    # 构造 RDD（与 range_basic 使用相同的数据生成逻辑）
    if args.input_type == "synthetic":
        rdd = build_synthetic_rdd(sc, args.num_records, args.num_partitions)
    elif args.input_type == "uniform":
        rdd = build_uniform_rdd_generated(
            sc,
            num_records=args.num_records,
            num_partitions=args.num_partitions,
            num_keys=NUM_KEYS,
        )
    elif args.input_type == "skewed":
        rdd = build_skewed_rdd_generated(
            sc,
            num_records=args.num_records,
            num_partitions=args.num_partitions,
            num_keys=NUM_KEYS,
            skew_ratio=SKEW_RATIO,
            hot_key=HOT_KEY,
        )
    else:
        raise ValueError(f"Unsupported input-type: {args.input_type}")

    hot_keys = parse_hot_keys(args.hot_keys)

    # 如果用户没有显式指定热点 key 且当前是倾斜数据，
    # 则自动将数据生成时使用的 HOT_KEY 作为热点 key。
    # 注意：hot_bucket_factor 完全由命令行参数控制，这里不做自动放大，
    # 以便实验结果中的 bX 与实际使用的桶数一致。
    if not hot_keys and args.input_type == "skewed":
        hot_keys = {HOT_KEY}

    # 运行实验
    metrics = run_custom_experiment(
        sc,
        rdd,
        num_partitions=args.num_partitions,
        hot_keys=hot_keys,
        hot_bucket_factor=args.hot_bucket_factor,
    )
    result_rdd: RDD = metrics.pop("result_rdd")

    # 可选样本输出
    if args.print_sample:
        result_sample = sorted(result_rdd.collect())[:20]
    else:
        result_sample = []

    # 指标打印
    print("----- Metrics (Custom Partitioner) -----")
    print(f"t_partition_seconds      : {metrics['t_partition']:.6f}")
    print(f"t_agg_seconds            : {metrics['t_agg']:.6f}")
    print(f"t_total_seconds_approx   : {metrics['t_total']:.6f}")

    print("partition_distribution_before (pid -> count):")
    for pid, cnt in metrics["partition_distribution_before"]:
        print(f"  {pid} -> {cnt}")

    print("partition_distribution_after_custom (pid -> count):")
    for pid, cnt in metrics["partition_distribution_after_custom"]:
        print(f"  {pid} -> {cnt}")

    print("partition_distribution_after_reduce_raw (pid -> count):")
    for pid, cnt in metrics["partition_distribution_after_reduce_raw"]:
        print(f"  {pid} -> {cnt}")

    print("partition_distribution_final (pid -> count):")
    for pid, cnt in metrics["partition_distribution_final"]:
        print(f"  {pid} -> {cnt}")

    if result_sample:
        print("Result sample (key -> sum), first 20 keys:")
        for k, v in result_sample:
            print(f"  {k} -> {v}")

    sc.stop()


if __name__ == "__main__":
    main()
