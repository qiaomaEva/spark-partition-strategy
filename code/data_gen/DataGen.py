from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 配置参数
NUM_RECORDS = 10_000_000   # 总行数，例如 1000 万
NUM_KEYS = 1000            # Key 范围：0 ~ 999
OUTPUT_BASE = "/home/yyw/spark-partition-strategy/data"  # 本地路径

# 不写 master，交给 spark-submit 控制
spark = SparkSession.builder \
    .appName("DataGenerator") \
    .getOrCreate()

def generate_uniform_data():
    print("正在生成均匀分布数据...")
    df = spark.range(0, NUM_RECORDS).select(
        (F.rand() * NUM_KEYS).cast("int").alias("key"),
        F.rpad(F.lit("val"), 50, "x").alias("value")
    )
    df.write.mode("overwrite").csv(f"{OUTPUT_BASE}/uniform_data")
    print("均匀数据生成完毕。")

def generate_skewed_data(skew_ratio=0.8, hot_key=0):
    print(f"正在生成倾斜数据 (倾斜度: {skew_ratio})...")
    skew_count = int(NUM_RECORDS * skew_ratio)
    df_skew = spark.range(0, skew_count).select(
        F.lit(hot_key).alias("key"),
        F.rpad(F.lit("val_hot"), 50, "x").alias("value")
    )
    remain_count = NUM_RECORDS - skew_count
    df_remain = spark.range(0, remain_count).select(
        (F.rand() * (NUM_KEYS - 1) + 1).cast("int").alias("key"),
        F.rpad(F.lit("val_norm"), 50, "x").alias("value")
    )
    final_df = df_skew.union(df_remain)
    final_df.write.mode("overwrite").csv(f"{OUTPUT_BASE}/skewed_data")
    print("倾斜数据生成完毕。")

if __name__ == "__main__":
    generate_uniform_data()
    generate_skewed_data(skew_ratio=0.85)
    spark.stop()
