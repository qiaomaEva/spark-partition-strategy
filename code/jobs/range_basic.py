from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.rdd import RDD

def main():
    # 1. 基本配置：不写 master，让 spark-submit 控制
    conf = SparkConf().setAppName("RangePartitionerBasic")
    sc = SparkContext.getOrCreate(conf)

    # 2. 构造一个简单的 (key, value) RDD
    #   这里先用本地数据，后面可以改成从 CSV / DataFrame 读
    data = [(i % 10, i) for i in range(100)]
    rdd = sc.parallelize(data, numSlices=4)

    # 3. 使用 RangePartitioner 对 key 进行重新分区
    #   说明：RangePartitioner 是 Scala 侧的类，在 PySpark 里常见的做法之一
    #   是通过 sortByKey + partitionBy 的组合来模拟“按 key 范围分布”的效果。
    #   为了先给你一个可跑的示例，我们重点看 repartition / sortByKey 的行为。
    #
    #   如果你们老师希望“严格意义上的 RangePartitioner 类”，
    #   后面可以通过 Py4J 调用 Scala 类，那个我们再单独细化。
    #
    sorted_rdd: RDD = rdd.sortByKey(numPartitions=4)

    # 4. 做一个简单的聚合，观察 Shuffle 行为
    #    比如对每个 key 求和
    reduced_rdd = sorted_rdd.reduceByKey(lambda x, y: x + y)

    # 5. 收集结果并打印（调试用，小数据）
    result = reduced_rdd.collect()
    print("Range-like partition result (key -> sum):")
    for k, v in result:
        print(k, "->", v)

    sc.stop()


if __name__ == "__main__":
    main()