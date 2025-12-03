from typing import List, Tuple

from pyspark import SparkContext
from pyspark.rdd import RDD

# 统一的数据分布相关常量
# - NUM_KEYS: key 取值范围 0 ~ NUM_KEYS-1
# - SKEW_RATIO: 倾斜数据中，多少比例的记录落在热点 key 上
# - HOT_KEY: 倾斜数据中的热点 key
NUM_KEYS: int = 1000
SKEW_RATIO: float = 0.95
HOT_KEY: int = 0


def build_synthetic_rdd(sc: SparkContext, num_records: int, num_partitions: int) -> RDD:
    """构造一个简单的 (key, value) RDD：
    - key: 0 ~ 9 循环
    - value: 序号本身
    用于最简单的功能验证。
    """
    data = [(i % 10, i) for i in range(num_records)]
    return sc.parallelize(data, numSlices=num_partitions)


def build_uniform_rdd_generated(
    sc: SparkContext,
    num_records: int,
    num_partitions: int,
    num_keys: int = NUM_KEYS,
) -> RDD:
    """在 Spark 中生成“均匀”分布的 (key, value) RDD。

    实现与 range_basic / hash_basic / custom_basic 保持一致：
    - 用 sc.range 生成 [0, num_records) 的 long 序列
    - key = i % num_keys
    - value = 1
    """
    base_rdd = sc.range(0, num_records, numSlices=num_partitions)

    def map_to_kv(i: int):
        k = int(i % num_keys)
        v = 1
        return k, v

    return base_rdd.map(map_to_kv)


def build_skewed_rdd_generated(
    sc: SparkContext,
    num_records: int,
    num_partitions: int,
    num_keys: int = NUM_KEYS,
    skew_ratio: float = SKEW_RATIO,
    hot_key: int = HOT_KEY,
) -> RDD:
    """在 Spark 中生成带热点 key 的倾斜 (key, value) RDD。

    思路与各 basic 脚本保持一致：
    - 总记录数 num_records
    - 前 skew_ratio * num_records 条记录都给热点 key（hot_key）
    - 剩余记录均匀分布到其它 key（1..num_keys-1），避免与 hot_key 冲突
    - value = 1
    """
    base_rdd = sc.range(0, num_records, numSlices=num_partitions)
    threshold = int(num_records * skew_ratio)

    def map_to_kv(i: int):
        if i < threshold:
            k = hot_key
        else:
            k = 1 + int(i % (num_keys - 1))
        v = 1
        return k, v

    return base_rdd.map(map_to_kv)


def compute_partition_distribution(rdd: RDD) -> List[Tuple[int, int]]:
    """统计每个分区的记录数：返回 [(partition_id, count), ...]，按 partition_id 排序。"""

    def count_in_partition(pid: int, it):
        cnt = 0
        for _ in it:
            cnt += 1
        yield pid, cnt

    dist_rdd = rdd.mapPartitionsWithIndex(count_in_partition)
    return sorted(dist_rdd.collect(), key=lambda x: x[0])
