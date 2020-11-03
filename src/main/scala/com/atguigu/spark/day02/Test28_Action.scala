package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-24 20:15
 */
/*
行动算子
1.reduce
  1)只有kv类型的RDD才可以通过隐式转换使用reduceByKey算子;
2.collect
  1)collect 以数组的形式返回数据集，收集数据时是按照分区收集，所以有序
  2)执行collect时，首先会将各个分区的数据转换为数组，再将各个Executor的数组汇总到Driver端连接成一个大数组，
    所以要慎用，避免内存不足。
  3）但是reduce是先在分区内聚合，在到Driver聚合，所以压力会小很多。
3.foreach，是直接在每个Executor上执行foreach，在执行时是并行的，不会将结果收集到Driver端，
  所以顺序可能不会按照放的顺序。因为分区内的数据，可能被打断了。
4.行动算子4：count 获取RDD中元素的个数,用的不多，因为使用后就直接提交作业。
5.行动算子5：first 返回RDD中的第一个元素
6.行动算子6：take 返回rdd 前n个元素组成的数组
7.行动算子7： takeOrdered 返回RDD排序后, 前n个元素组成的数组
8.行动算子8： aggregate（初始值）（分区内计算规则，分区间计算规则）
    // 注意：初始值不止在分区内计算时，使用一次，而且在分区间计算时还要使用时一次初始值。与aggregateByKey不一样。
    // 注意：在有些场景，分区内和分区间都会用到初始值，比如财务系统的基准值。
    // 注意：aggregate可以针对单值，但是aggregateByKey只能针对KV类型
9.行动算子9： fold（初始值）（分区内间计算规则）,fold是aggregate的简化，分区内和分区间的计算规则一致。
10.行动算子10： countByKey 统计每种key出现的个数
11.行动算子11： countByValue 统计每种元素出现的个数
12.save相关算子：
  1）saveAsTextFile（路径），保存为文本文件，要求输出路径不能存在
  2）saveAsObjectFile（路径），保存为序列化文件
  3）saveAsSequenceFile（路径），保存为Sequence文件格式，处理小文件，注意只支持KV类型的RDD
13.注意：一个应用程序中可能有多个job被提交，触发行动算子，底层就会new 一个job提交。一个应用程序可以提交多个job
14.读取文件：创建RDD不止是textFile，还可以是sequenceFile，或者 objectFile(读取序列化形式的文件)
15.分区和分组之间没有关系，分区是决定RDD的并行度，将RDD中的数据分配到多个Executor上执行，而分组是一个分区可以有多个组。
 */
object Test28_Action {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    //行动算子1：reduce
    val res: Int = rdd.reduce(_+_)
    println(res) // 10

    //行动算子2：collect 以数组的形式返回数据集，收集数据时是按照分区收集，所以有序
    val ints: Array[Int] = rdd.collect()
    ints.foreach(println) // 1 2 3 4

    //行动算子3：foreach，是直接在每个Executor上执行foreach，在执行时是并行的，不会将结果收集到Driver端，
    // 所以顺序可能不会按照放的顺序。因为分区内的数据，可能被打断了。
    rdd.foreach(println) // 3 2 1 4

    //行动算子4：count 获取RDD中元素的个数,用的不多，因为使用后就直接提交作业。
    val res1: Long = rdd.count()
    println(res1) // 4

    //行动算子5：first 返回RDD中的第一个元素
    val res3: Int = rdd.first()
    println(res3) //1

    //行动算子6：take 返回rdd 前n个元素组成的数组
    val ints1: Array[Int] = rdd.take(2)
    ints1.mkString(",") // 1,2

    //行动算子7： takeOrdered 返回RDD排序后, 前n个元素组成的数组
    val rdd2: RDD[Int] = sc.makeRDD(List(1,6,5,3,2),2)
    val ints2: Array[Int] = rdd2.takeOrdered(3)
    println(ints2.mkString(",")) // 1 2 3

    //行动算子8： aggregate（初始值）（分区内计算规则，分区间计算规则）
    // 注意：初始值不止在分区内计算时，使用一次，而且在分区间计算时还要使用时一次初始值。与aggregateByKey不一样。
    // 注意：在有些场景，分区内和分区间都会用到初始值，比如财务系统的基准值。
    // 注意：aggregate可以针对单值，但是aggregateByKey只能针对KV类型
    val rdd4: RDD[Int] = sc.makeRDD(List(1,2,3,4),8)
    val res6: Int = rdd4.aggregate(0)(_+_,_+_)
    println(res6) //10
    val res7: Int = rdd4.aggregate(10)(_+_,_+_)
    println(res7) //100 = (10 + (10 + 1) + 10 + (10+2) + 10 + (10+3) + 10 + 14 ) +10

    //行动算子9： fold（初始值）（分区内间计算规则）
    // fold是aggregate的简化，分区内和分区间的计算规则一致。
    val rdd7: Int = rdd4.fold(10)(_+_)
    println(rdd7) // 100

    //行动算子10： countByKey 统计每种key出现的个数
    val rdd8: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
    val res10: collection.Map[Int, Long] = rdd8.countByKey()
    println(res10) // Map(1 -> 3, 2 -> 1, 3 -> 2)

    //行动算子11： countByValue 统计每种元素出现的个数
    val tupleToLong: collection.Map[(Int, String), Long] = rdd8.countByValue()
    println(tupleToLong) // Map((2,a) -> 1, (3,c) -> 2, (1,a) -> 2, (2,b) -> 1)

    //save相关算子
    //行动算子12：saveAsTextFile（路径），保存为文本文件，要求输出路径不能存在
    val rdd11: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val unit: Unit = rdd11.saveAsTextFile("output")

    //行动算子13：saveAsObjectFile（路径），保存为序列化文件
    rdd11.saveAsObjectFile("output1")

    //行动算子14：saveAsSequenceFile（路径），保存为Sequence文件格式，处理小文件，注意只支持KV类型的RDD
    rdd11.map((_,1)).saveAsSequenceFile("output2")

    //注意：一个应用程序中可能有多个job被提交，触发行动算子，底层就会new 一个job提交。一个应用程序可以提交多个job

    //读取文件：创建RDD不止是textFile，还可以是sequenceFile，或者 objectFile(读取序列化形式的文件)
//    sc.objectFile("output1")
//    sc.sequenceFile("output2")

    // 3.关闭资源
    sc.stop()
  }

}
