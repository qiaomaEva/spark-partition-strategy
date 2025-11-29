import os
from datetime import datetime
import argparse
import time
from typing import Literal, Dict, Any, List, Tuple

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD

# 为了和 DataGen.py 中的设定保持一致，这里写几个常量：
# - NUM_KEYS: key 的取值范围 0 ~ NUM_KEYS-1
# - SKEW_RATIO: 倾斜数据中，多少比例的记录落在热点 key 上
# - HOT_KEY: 倾斜数据中的热点 key
NUM_KEYS = 1000
SKEW_RATIO = 0.85
HOT_KEY = 0


def parse_args() -> argparse.Namespace:
    """
    解析命令行参数：
      --input-type: synthetic / uniform / skewed（数据生成模式）
      --num-records: 生成多少条记录
      --num-partitions: RDD 的分区数
      --print-sample: 是否打印结果样本（仅本地调试用）
    """
    parser = argparse.ArgumentParser(
        description="Basic Range-like partitioning experiment (by C)."
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
        help="number of partitions for the RDD / sortByKey.",
    )

    parser.add_argument(
        "--print-sample",
        action="store_true",
        help="print sample of (key, sum) result for debugging.",
    )

    args = parser.parse_args()
    return args


def build_synthetic_rdd(sc: SparkContext, num_records: int, num_partitions: int) -> RDD:
    """
    构造一个简单的 (key, value) RDD：
      - key: 0 ~ 9 循环
      - value: 序号本身
    用于最简单的功能验证。
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

    这里为了简单和可控，用一种确定性但近似均匀的方式：
      - 先用 sc.range 生成 [0, num_records) 的 long 序列
      - key = i % num_keys   （i 是记录序号）
      - value = 1
    这样每个 key 出现的次数基本相同，相当于均匀分布。
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

    思路和 DataGen.py 保持一致：
      - 总记录数 num_records
      - 其中前 skew_ratio * num_records 条记录都给热点 key（hot_key）
      - 剩下的记录均匀分布在其它 key（1..num_keys-1）上
      - value = 1
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


def compute_partition_distribution(rdd: RDD) -> List[Tuple[int, int]]:
    """
    统计每个分区的记录数：返回 [(partition_id, count), ...]，按 partition_id 排序。
    思路：
      - mapPartitionsWithIndex 拿到 (pid, iterator)
      - 对每个分区数一下元素个数
    """
    def count_in_partition(pid: int, it):
        cnt = 0
        for _ in it:
            cnt += 1
        yield pid, cnt

    dist_rdd = rdd.mapPartitionsWithIndex(count_in_partition)
    return sorted(dist_rdd.collect(), key=lambda x: x[0])


def run_range_experiment(sc: SparkContext, rdd: RDD, num_partitions: int) -> Dict[str, Any]:
    """
    执行 Range-like 实验，并返回一组指标：
      - t_sort: sortByKey 的耗时
      - t_agg: reduceByKey 的耗时
      - t_total: 两者总和（近似）
      - partition_distribution_before: sortByKey 之前每个分区的记录数
      - partition_distribution_after:  sortByKey + reduceByKey 之后每个分区的记录数
      - result_sample: (key, sum) 的小样本（仅用于本地调试）
    """
    metrics: Dict[str, Any] = {}

    # 1. sortByKey 前的分区分布
    partition_dist_before = compute_partition_distribution(rdd)
    metrics["partition_distribution_before"] = partition_dist_before

    # 2. sortByKey
    t0 = time.time()
    sorted_rdd: RDD = rdd.sortByKey(numPartitions=num_partitions)
    t1 = time.time()
    metrics["t_sort"] = t1 - t0

    # 3. sortByKey 后的分区分布
    partition_dist_after_sort = compute_partition_distribution(sorted_rdd)
    metrics["partition_distribution_after_sort"] = partition_dist_after_sort

    # 4. reduceByKey
    t2 = time.time()
    reduced_rdd: RDD = sorted_rdd.reduceByKey(lambda x, y: x + y)
    t3 = time.time()
    metrics["t_agg"] = t3 - t2
    metrics["t_total"] = (t1 - t0) + (t3 - t2)

    # 5. reduce 后的分区分布
    partition_dist_after_reduce = compute_partition_distribution(reduced_rdd)
    metrics["partition_distribution_after_reduce"] = partition_dist_after_reduce

    # 6. 返回最终 RDD 以便上层需要时 collect
    metrics["result_rdd"] = reduced_rdd

    return metrics


def main():
    args = parse_args()

    # 为当前实验 run 生成一个唯一 ID（方便多次运行区分）
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Spark 事件日志目录（保存到项目根目录下的 logs/spark-events）
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    event_log_dir = os.path.join(project_root, "logs", "spark-events")
    event_log_dir = os.path.abspath(event_log_dir)

    conf = (
        SparkConf()
        .setAppName(f"RangePartitionerBasic-{run_id}")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", f"file://{event_log_dir}")
    )
    # 确保事件日志目录存在
    os.makedirs(event_log_dir, exist_ok=True)
    sc = SparkContext.getOrCreate(conf)

    print("===== RangePartitioner Basic Experiment (by C) =====")
    print(f"Input type     : {args.input_type}")
    print(f"Num records    : {args.num_records}")
    print(f"Num partitions : {args.num_partitions}")
    print(f"Print sample   : {args.print_sample}")

    # 1. 根据输入类型 input-type 构造 RDD
    if args.input_type == "synthetic":
        rdd = build_synthetic_rdd(sc, args.num_records, args.num_partitions)
    elif args.input_type == "uniform":
        # 在 Spark 内部生成均匀数据，而不是读 CSV
        rdd = build_uniform_rdd_generated(
            sc,
            num_records=args.num_records,
            num_partitions=args.num_partitions,
            num_keys=NUM_KEYS,
        )
    elif args.input_type == "skewed":
        # 在 Spark 内部生成倾斜数据，而不是读 CSV
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

    # 2. 运行实验并收集指标
    metrics = run_range_experiment(sc, rdd, args.num_partitions)
    result_rdd: RDD = metrics.pop("result_rdd")  # 从字典中取出，避免重复存大对象

    # 3. 收集结果小样本（仅用于本地调试）
    if args.print_sample:
        result_sample = sorted(result_rdd.collect())[:20]
    else:
        result_sample = []

    # 4. 结构化打印指标
    print("----- Metrics -----")
    print(f"t_sort_seconds           : {metrics['t_sort']:.6f}")
    print(f"t_agg_seconds            : {metrics['t_agg']:.6f}")
    print(f"t_total_seconds_approx   : {metrics['t_total']:.6f}")

    print("partition_distribution_before (pid -> count):")
    for pid, cnt in metrics["partition_distribution_before"]:
        print(f"  {pid} -> {cnt}")

    print("partition_distribution_after_sort (pid -> count):")
    for pid, cnt in metrics["partition_distribution_after_sort"]:
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
