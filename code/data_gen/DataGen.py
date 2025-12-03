import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==========================================
# 常量定义
# ==========================================
NUM_KEYS = 1000
HOT_KEY = 0
# ==========================================

def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic datasets (Standalone v2).")
    
    parser.add_argument(
        "--output-path", 
        type=str, 
        required=True, 
        help="Local path to save the generated data"
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

    return parser.parse_args()

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