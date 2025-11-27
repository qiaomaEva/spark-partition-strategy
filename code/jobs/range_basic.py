import argparse
import time

from typing import Literal

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD


def parse_args() -> argparse.Namespace:
    """
    解析命令行参数：
      --input-type: 数据来源类型（synthetic / uniform / skewed）
      --num-records: 生成多少条记录（仅 synthetic 模式下使用）
      --num-partitions: RDD 的分区数
      --input-path: 从文件读数据时的根目录
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
InputKind = Literal["uniform", "skewed"]


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


def run_range_experiment(sc: SparkContext, rdd: RDD, num_partitions: int) -> RDD:
    """
    使用 sortByKey(numPartitions=...) 模拟“按 key 范围划分”的行为，
    然后对每个 key 做 reduceByKey 聚合。
    """
    # sortByKey 会触发一个 shuffle，并根据 numPartitions 重新分区
    sorted_rdd: RDD = rdd.sortByKey(numPartitions=num_partitions)

    # 做一个简单聚合：对每个 key 求和
    reduced_rdd: RDD = sorted_rdd.reduceByKey(lambda x, y: x + y)

    return reduced_rdd


def main():
    args = parse_args()

    conf = SparkConf().setAppName("RangePartitionerBasic")
    sc = SparkContext.getOrCreate(conf)

    print("===== RangePartitioner Basic Experiment (by C) =====")
    print(f"Input type     : {args.input_type}")
    print(f"Num records    : {args.num_records}")
    print(f"Num partitions : {args.num_partitions}")

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

    start_time = time.time()
    result_rdd = run_range_experiment(sc, rdd, args.num_partitions)
    result = result_rdd.collect()
    end_time = time.time()

    print("Result (key -> sum) for small-scale debug:")
    # 为避免本地调试时输出太多，只打印前几个 key
    for k, v in sorted(result)[:20]:
        print(k, "->", v)

    duration = end_time - start_time
    print(f"Total job time (approx): {duration:.3f} seconds")

    sc.stop()


if __name__ == "__main__":
    main()