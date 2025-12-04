from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

"""公共的 RDD 加载与分区统计工具。

目前只在 jobs 中复用，不再在这里维护数据分布相关常量，
避免与 data_gen 中的生成配置产生隐式耦合。
"""


from typing import Optional


def load_rdd_from_parquet(spark: SparkSession, input_path: str, num_partitions: Optional[int] = None) -> RDD:
    """
    从 Parquet 文件加载数据并转换为 (key, value) 的 RDD。
    
    Args:
        spark: SparkSession 对象
        input_path: 数据路径 (例如 file:///data/uniform)
        num_partitions: 如果指定，读取后进行 repartition，保证实验并行度一致
    """
    # 读取 Parquet
    df = spark.read.parquet(input_path)
    
    # 转换为 RDD: Row(key=..., value=...) -> (key, value)
    rdd = df.rdd.map(lambda row: (row.key, row.value))
    
    # 如果需要强制分区数（实验通常需要固定的分区数来控制变量）
    if num_partitions is not None:
        current_partitions = rdd.getNumPartitions()
        if current_partitions != num_partitions:
            # 使用 coalesce 减少分区，或 repartition 增加分区/shuffle
            rdd = rdd.repartition(num_partitions)
            
    return rdd


def compute_partition_distribution(rdd: RDD) -> List[Tuple[int, int]]:
    """统计每个分区的记录数：返回 [(partition_id, count), ...]，按 partition_id 排序。"""

    def count_in_partition(pid: int, it):
        cnt = 0
        for _ in it:
            cnt += 1
        yield pid, cnt

    dist_rdd = rdd.mapPartitionsWithIndex(count_in_partition)
    return sorted(dist_rdd.collect(), key=lambda x: x[0])