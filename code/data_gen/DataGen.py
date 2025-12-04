import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==========================================
# 常量定义（集中配置，简化命令行参数）
# ==========================================
NUM_KEYS = 1000
HOT_KEY = 0

# 数据集输出根目录（父目录），具体子目录名根据参数自动生成：
#   <type>_<records_tag>_ratio<skew_ratio>
# 例如：
#   type=skewed, num_records=5_000_000, skew_ratio=0.95
#   → records_tag = "5m"
#   → 目录名 = "skewed_5m_ratio0.95"
#   → 最终输出路径 = "file:///home/admin/spark-data/skewed_5m_ratio0.95"
OUTPUT_ROOT = "file:///home/admin/spark-data"
# ==========================================


def _build_records_tag(num_records: int) -> str:
    """将记录数转为人类友好的 tag，例如 5_000_000 → "5m"，2_000_000 → "2m"。"""
    if num_records % 1_000_000 == 0:
        return f"{num_records // 1_000_000}m"
    if num_records % 1_000 == 0:
        return f"{num_records // 1_000}k"
    return str(num_records)


def _build_dataset_name(args) -> str:
    """根据命令行参数生成目录名：<type>_<records_tag>_ratio<skew_ratio>。

    为了和 shell 脚本中的推导规则保持一致，这里直接用原始的 skew_ratio
    字符串形式（不再格式化为两位小数），例如：

    - skew_ratio=0.0   → uniform_2m_ratio0.0
    - skew_ratio=0.75  → skewed_5m_ratio0.75
    - skew_ratio=0.95  → skewed_5m_ratio0.95
    """
    records_tag = _build_records_tag(args.num_records)
    # 使用原始参数字符串，避免 0.0/0.00 等差异造成目录名不一致
    ratio_tag = str(args.skew_ratio)
    return f"{args.type}_{records_tag}_ratio{ratio_tag}"

def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic datasets (Standalone v2).")

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
        default=5000000, 
        help="Number of records to generate."
    )
    
    parser.add_argument(
        "--num-partitions", 
        type=int, 
        default=32, 
        help="Number of partitions for the output file."
    )
    
    parser.add_argument(
        "--skew-ratio", 
        type=float, 
        default=0.95, 
        help="Ratio of records for the hot key (default 0.95)."
    )

    args = parser.parse_args()

    # 根据参数自动生成数据集目录名，规则见文件头注释：
    #   <type>_<records_tag>_ratio<skew_ratio>
    # 然后拼到 OUTPUT_ROOT 下作为最终输出路径。
    dataset_name = _build_dataset_name(args)
    args.output_path = f"{OUTPUT_ROOT}/{dataset_name}"
    return args

def generate_data(spark: SparkSession, args):
    print(f"Generating {args.type} data: {args.num_records} records, Skew Ratio: {args.skew_ratio}")
    print(f"Output to: {args.output_path}")
    
    # 1. 生成基础序列
    df = spark.range(0, args.num_records, step=1, numPartitions=args.num_partitions)
    
    # 2. 根据类型计算 Key
    if args.type == "uniform":
        df_result = df.select(
            (F.col("id") % NUM_KEYS).cast("int").alias("key"),
            F.lit(1).alias("value")
        )
    
    elif args.type == "skewed":
        # 计算热点数据的阈值
        threshold = int(args.num_records * args.skew_ratio)
        
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
    df_result.write.mode("overwrite").parquet(args.output_path)
    print(f"Success! Data saved to {args.output_path}")

def main():
    args = parse_args()
    
    # 初始化 SparkSession
    spark = (SparkSession.builder
             .appName("DataGeneratorStandalone_v2")
             .master("local[*]")
             .getOrCreate())
    
    generate_data(spark, args)
    spark.stop()

if __name__ == "__main__":
    main()