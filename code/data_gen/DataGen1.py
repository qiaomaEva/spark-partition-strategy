import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 引用 common 中的常量配置，保证逻辑一致
# 注意：你需要确保运行此脚本时 python path 能找到 jobs.common
# 或者你可以手动将 common.py 中的常量复制过来
NUM_KEYS = 1000
SKEW_RATIO = 0.85
HOT_KEY = 0


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic datasets for Spark experiments.")

    parser.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="Local path to save the generated data (e.g., /tmp/spark_data/uniform_1m)"
    )

    parser.add_argument(
        "--type",
        type=str,
        choices=["uniform", "skewed"],
        required=True,
        help="Type of data distribution."
    )

    parser.add_argument(
        "--num-records",
        type=int,
        default=100000,
        help="Number of records to generate."
    )

    parser.add_argument(
        "--num-partitions",
        type=int,
        default=4,
        help="Number of partitions for the output file."
    )

    return parser.parse_args()


def generate_data(spark: SparkSession, args):
    print(f"Generating {args.type} data: {args.num_records} records, output to {args.output_path}")

    # 1. 生成基础序列 0 ~ num_records-1
    # distinct range ensures we control the exact number of rows
    df = spark.range(0, args.num_records, step=1, numPartitions=args.num_partitions)

    # 2. 根据类型计算 Key
    if args.type == "uniform":
        # Key = id % NUM_KEYS
        # Value = 1
        df_result = df.select(
            (F.col("id") % NUM_KEYS).cast("int").alias("key"),
            F.lit(1).alias("value")
        )

    elif args.type == "skewed":
        # 倾斜逻辑与 common.py 中保持一致:
        # threshold = num_records * skew_ratio
        # if id < threshold: key = hot_key
        # else: key = 1 + (id % (NUM_KEYS - 1))

        threshold = int(args.num_records * SKEW_RATIO)

        # 使用 Spark SQL 表达式实现
        df_result = df.select(
            F.when(
                F.col("id") < threshold,
                F.lit(HOT_KEY)
            ).otherwise(
                1 + (F.col("id") % (NUM_KEYS - 1))
            ).cast("int").alias("key"),
            F.lit(1).alias("value")
        )

    else:
        raise ValueError(f"Unknown type: {args.type}")

    # 3. 写入 Parquet
    # mode("overwrite") 会覆盖已存在的目录
    df_result.write.mode("overwrite").parquet(args.output_path)
    print(f"Success! Data saved to {args.output_path}")


def main():
    args = parse_args()

    # 初始化 SparkSession
    spark = (SparkSession.builder
             .appName("DataGenerator")
             .master("local[*]")  # 生成数据通常在单机跑即可，生成后再分发
             .getOrCreate())

    generate_data(spark, args)
    spark.stop()


if __name__ == "__main__":
    main()
