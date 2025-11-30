import os
from datetime import datetime
import argparse
import time
from typing import Dict, Any, List, Tuple

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD

# 为了和 DataGen / range_basic / custom_basic 设定保持一致：
# - NUM_KEYS: key 取值范围 0 ~ NUM_KEYS-1
# - SKEW_RATIO: 倾斜数据中，多少比例的记录落在热点 key 上
# - HOT_KEY: 倾斜数据中的热点 key
NUM_KEYS = 1000
SKEW_RATIO = 0.85
HOT_KEY = 0


def parse_args() -> argparse.Namespace:
    """
    解析命令行参数 (Hash Experiment by B)：
      --input-type: synthetic / uniform / skewed（数据生成模式）
      --num-records: 生成多少条记录
      --num-partitions: RDD 的分区数
      --print-sample: 是否打印结果样本（仅本地调试用）
    """
    parser = argparse.ArgumentParser(
        description="Basic Hash Partitioning experiment (by B)."
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
        help="number of partitions for the RDD / partitionBy.",
    )

    parser.add_argument(
        "--print-sample",
        action="store_true",
        help="print sample of (key, sum) result for debugging.",
    )

    args = parser.parse_args()
    return args


# ---------- 数据构造 (与 range/custom 保持完全一致以控制变量) ----------

def build_synthetic_rdd(sc: SparkContext, num_records: int, num_partitions: int) -> RDD:
    """
    构造一个简单的 (key, value) RDD：
      - key: 0 ~ 9 循环
      - value: 序号本身
    """
    data = [(i % 10, i) for i in range(num_records)]
    rdd = sc.parallelize(data, numSlices=num_partitions)
    return rdd


def build_uniform_rdd_generated(
    sc: SparkContext,
    num_records: int,
    num_partitions: int,
    num_keys: int = NUM_KEYS,
) -> RDD:
    """
    在 Spark 中生成“均匀”分布的 (key, value) RDD。
    """
    base_rdd = sc.range(0, num_records, numSlices=num_partitions)

    def map_to_kv(i: int):
        k = int(i % num_keys)
        v = 1
        return k, v

    rdd = base_rdd.map(map_to_kv)
    return rdd


def build_skewed_rdd_generated(
    sc: SparkContext,
    num_records: int,
    num_partitions: int,
    num_keys: int = NUM_KEYS,
    skew_ratio: float = SKEW_RATIO,
    hot_key: int = HOT_KEY,
) -> RDD:
    """
    在 Spark 中生成带热点 key 的倾斜 (key, value) RDD。
    """
    base_rdd = sc.range(0, num_records, numSlices=num_partitions)
    threshold = int(num_records * skew_ratio)

    def map_to_kv(i: int):
        if i < threshold:
            k = hot_key
        else:
            # 分布到 1..num_keys-1 之间，避免和 hot_key 冲突
            k = 1 + int(i % (num_keys - 1))
        v = 1
        return k, v

    rdd = base_rdd.map(map_to_kv)
    return rdd


# ---------- 统计工具 ----------

def compute_partition_distribution(rdd: RDD) -> List[Tuple[int, int]]:
    """
    统计每个分区的记录数：返回 [(partition_id, count), ...]，按 partition_id 排序。
    这通常是一个 Action 操作，会触发前面的 Transformation 执行。
    """
    def count_in_partition(pid: int, it):
        cnt = 0
        for _ in it:
            cnt += 1
        yield pid, cnt

    dist_rdd = rdd.mapPartitionsWithIndex(count_in_partition)
    return sorted(dist_rdd.collect(), key=lambda x: x[0])


# ---------- 核心实验逻辑 (Hash Partitioner) ----------

def run_hash_experiment(sc: SparkContext, rdd: RDD, num_partitions: int) -> Dict[str, Any]:
    """
    执行 Hash Partitioning 实验，并返回一组指标：
      - t_partition: partitionBy (Hash) 的耗时
      - t_agg: reduceByKey 的耗时
      - t_total: 两者总和
      - 分区分布变化情况
    """
    metrics: Dict[str, Any] = {}

    # 1. PartitionBy 之前的分区分布
    metrics["partition_distribution_before"] = compute_partition_distribution(rdd)

    # 2. PartitionBy (Hash)
    # PySpark 的 rdd.partitionBy 默认使用 portable_hash，即 HashPartitioner
    t0 = time.time()
    partitioned_rdd: RDD = rdd.partitionBy(numPartitions=num_partitions)
    t1 = time.time()
    metrics["t_partition"] = t1 - t0

    # 3. PartitionBy 后的分区分布
    # 注意：调用 compute_partition_distribution 会触发 Shuffle
    metrics["partition_distribution_after_hash"] = compute_partition_distribution(partitioned_rdd)

    # 4. reduceByKey
    # 由于数据已经按 Hash 分区，理论上 reduceByKey 这里的 Shuffle 开销会很小（主要是 map-side combine 后直接聚合）
    # 除非 Spark 优化器决定重新 Shuffle
    t2 = time.time()

    grouped_rdd = partitioned_rdd.groupByKey()

    # 为了触发 Action 并计算时间，我们需要对结果做操作
    # mapValues(len) 计算每个 key 有多少条记录，count() 触发执行
    reduced_rdd = grouped_rdd.mapValues(len)
    reduced_rdd.count()  # 触发 Action

    t3 = time.time()
    metrics["t_agg"] = t3 - t2
    metrics["t_total"] = (t1 - t0) + (t3 - t2)

    # 5. reduce 后的分区分布
    metrics["partition_distribution_after_reduce"] = compute_partition_distribution(reduced_rdd)

    # 6. 返回结果 RDD
    metrics["result_rdd"] = reduced_rdd

    return metrics


# ---------- Main ----------

def main():
    args = parse_args()

    # 生成唯一 Run ID
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Spark 事件日志路径
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    event_log_dir = os.path.join(project_root, "logs", "spark-events")
    event_log_dir = os.path.abspath(event_log_dir)

    conf = (
        SparkConf()
        .setAppName(f"HashPartitionerBasic-{run_id}")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", f"file://{event_log_dir}")
    )
    os.makedirs(event_log_dir, exist_ok=True)
    sc = SparkContext.getOrCreate(conf)

    print("===== HashPartitioner Basic Experiment (by B) =====")
    print(f"Input type     : {args.input_type}")
    print(f"Num records    : {args.num_records}")
    print(f"Num partitions : {args.num_partitions}")
    print(f"Print sample   : {args.print_sample}")

    # 1. 构造 RDD
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

    # 2. 运行实验
    metrics = run_hash_experiment(sc, rdd, args.num_partitions)
    result_rdd: RDD = metrics.pop("result_rdd")

    # 3. 打印样本 (可选)
    if args.print_sample:
        result_sample = sorted(result_rdd.collect())[:20]
    else:
        result_sample = []

    # 4. 打印指标
    print("----- Metrics (Hash Partitioner) -----")
    print(f"t_partition_seconds      : {metrics['t_partition']:.6f}")
    print(f"t_agg_seconds            : {metrics['t_agg']:.6f}")
    print(f"t_total_seconds_approx   : {metrics['t_total']:.6f}")

    print("partition_distribution_before (pid -> count):")
    for pid, cnt in metrics["partition_distribution_before"]:
        print(f"  {pid} -> {cnt}")

    print("partition_distribution_after_hash (pid -> count):")
    for pid, cnt in metrics["partition_distribution_after_hash"]:
        print(f"  {pid} -> {cnt}")

    print("partition_distribution_after_reduce (pid -> count):")
    for pid, cnt in metrics["partition_distribution_after_reduce"]:
        print(f"  {pid} -> {cnt}")

    if result_sample:
        print("Result sample (key -> sum), first 20 keys:")
        for k, v in result_sample:
            print(f"  {k} -> {v}")

    sc.stop()


if __name__ == "__main__":
    main()
