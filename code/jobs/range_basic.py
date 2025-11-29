import os
from datetime import datetime

import argparse
import time
from typing import Literal, Dict, Any, List, Tuple

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD

InputKind = Literal["uniform", "skewed"]


def parse_args() -> argparse.Namespace:
    """
    解析命令行参数：
      --input-type: synthetic / uniform / skewed
      --num-records: 生成多少条记录（仅 synthetic 模式使用）
      --num-partitions: RDD 的分区数
      --input-path: 从文件读数据时的根目录
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
        help="input data type: synthetic (generated), uniform or skewed (from CSV files).",
    )

    parser.add_argument(
        "--num-records",
        type=int,
        default=100000,
        help="number of records to generate in synthetic mode.",
    )

    parser.add_argument(
        "--num-partitions",
        type=int,
        default=4,
        help="number of partitions for the RDD / sortByKey.",
    )

    parser.add_argument(
        "--input-path",
        type=str,
        default="data",
        help="root path of input data when input-type is uniform or skewed.",
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
    """
    data = [(i % 10, i) for i in range(num_records)]
    rdd = sc.parallelize(data, numSlices=num_partitions)
    return rdd

# 增加从 CSV 文件读入数据的函数
def build_rdd_from_csv(
    sc: SparkContext,
    input_root: str,
    kind: InputKind,
    num_partitions: int,
) -> RDD:
    """
    从 CSV 读入 (key, value) 形式的数据。

    假设目录结构类似：
      data/uniform_data/*.csv
      data/skewed_data/*.csv

    每行格式假定为: key,value
    如果你们真实数据格式不一样，可以按实际修改解析逻辑。
    """
    if kind == "uniform":
        path = f"{input_root}/uniform_data/*.csv"
    elif kind == "skewed":
        path = f"{input_root}/skewed_data/*.csv"
    else:
        raise ValueError(f"Unsupported kind: {kind}")

    # 读原始文本
    lines = sc.textFile(path, minPartitions=num_partitions)

    # 解析成 (key, value)；假设 key, value 都是整数
    def parse_line(line: str):
        parts = line.strip().split(",")
        if len(parts) < 2:
            return None
        try:
            k = int(parts[0])
            v = int(parts[1])
            return (k, v)
        except ValueError:
            return None

    parsed = lines.map(parse_line).filter(lambda x: x is not None)
    return parsed

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

# 核心实验逻辑
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

    # 5. reduce 后的分区分布（可选：一般 reduce 后分区不变，但数据条数减少）
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
    #
    os.makedirs(event_log_dir, exist_ok=True)
    sc = SparkContext.getOrCreate(conf)

    print("===== RangePartitioner Basic Experiment (by C) =====")
    print(f"Input type     : {args.input_type}")
    print(f"Num records    : {args.num_records}")
    print(f"Num partitions : {args.num_partitions}")
    print(f"Num partitions : {args.num_partitions}")

    # 1. 根据输入类型input-type构造RDD
    if args.input_type == "synthetic":
        rdd = build_synthetic_rdd(sc, args.num_records, args.num_partitions)
    elif args.input_type == "uniform":
        rdd = build_rdd_from_csv(
            sc,
            input_root=args.input_path,
            kind="uniform",
            num_partitions=args.num_partitions,
        )
    elif args.input_type == "skewed":
        rdd = build_rdd_from_csv(
            sc,
            input_root=args.input_path,
            kind="skewed",
            num_partitions=args.num_partitions,
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
    
    # 4. 结构化打印指标（方便复制到报告或导出为 JSON）
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