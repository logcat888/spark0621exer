package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-26 14:35
 */
/*
累加器：
1. 行动算子不存在Shuffle，因为Shuffle必须要new ShuffleRDD。
2.为什么叫分布式共享只读变量？ task之间不能读取数据
  因为在不同task上累加器是不能相互读的，但是在同一个task上累加器中的数据是可以读。
3. 对于sum，reduce这种针对单值聚合的行动算子，单值RDD，直接并对数据进行求和，不存在Shuffle过程。
   因为单值聚合，每个分区聚合后只有一个值，不会消耗内存，可以等待其他分区数据，没有必要Shuffle落盘，直接放在内存即可。
4. 但是kv类型聚合（reduceByKey、aggregateByKey..），必须要Shuffle落盘，因为一个分区内聚合完后数据量依然很大，同时还要依赖其他分区的数据，
    不可能一直放在内存中，所以要落盘，否则可能造成OOM。
5. Driver端的普遍变量对Executor端使用的影响？
  如果定义一个普通的变量，那么在Driver定义，Executor会创建变量的副本，算子都是对副本进行操作，Driver端的变量不会更新。
  存在的问题就是没有把Executor端的副本进行聚合，如果把副本进行聚合，那么问题就解决了。
6. 累加器种类：1）longAccumulator；2）doubleAccumulator；3）collectionAccumulator
7. 累加器添加值add() ，获取值value()
 */
object Test10_Accumulator {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    // 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    //单值RDD，直接并对数据进行求和，不存在Shuffle过程。
    //因为单值聚合，每个分区聚合后只有一个值，不会消耗内存，可以等待其他分区数据，没有必要Shuffle落盘，直接放在内存即可。
    //但是kv类型聚合，必须要Shuffle落盘，因为一个分区内聚合完后数据量依然很大，同时还要依赖其他分区的数据，
    // 不可能一直放在内存中，所以要落盘，否则可能造成OOM。
    println(rdd.sum()) // 10.0，没有Shuffle
    println(rdd.reduce(_ + _)) // 10，没有Shuffle

    //存在Shuffle，效率低
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",2),("a",3),("a",4),("a",1)),2)
    val resRDD1: RDD[(String, Int)] = rdd1.reduceByKey(_+_)
    resRDD1.collect().foreach(println) // 10

    //需求：实现累加效果
    // 如果定义一个普通的变量，那么在Driver定义，Executor会创建变量的副本，算子都是对副本进行操作，Driver端的变量不会更新。
    // 存在的问题就是没有把Executor端的副本进行聚合，如果把副本进行聚合，那么问题就解决了。
    var sum = 0 //Driver端的sum
    rdd1.foreach({
      case (word,count) => {  //现在假设只有a
        sum += count  // Executor端的sum副本，与Driver端的sum没有关系，与各Executor端的sum副本也没有关系
      }
    })
    println(sum) // 0  //Driver端的sum

    //如果需要通过Executor，对Driver端定义的变量进行更新，需要定义为累加器
    //累加器与普通变量相比，会将Executor端的结果收集到Driver端进行汇总，汇总的规则自己指定（相加、相乘、比较大小..）。
    // 与collect不同，collect只是把数据收集。而累加器只是收集动作与collect相同，把数据从各分区拿到Driver端，但是汇总的规则自己指定。
    // 同时IO也比collect小很多，因为已经在各分区内进行了汇总。

    //创建累加器
    val sum1: LongAccumulator = sc.longAccumulator("myAcc") //定义整型累加器的名字，此外还有DoubleAccumulator
    rdd1.foreach({
      case (word,count) => {
        sum1.add(count)
        println("--"+sum1.value+"--")  // 2个分区，数据 4 1 ， 2  3  //集合创建RDD指定分区中数据的流向
        /*
        --4--
        --5--
        --2--
        --5--
         */
        //分区内还是可以读累加器的
      }
    })
    println(sum1) //LongAccumulator(id: 125, name: Some(myAcc), value: 10)
    println(sum1.value) // 10

    // 3.关闭资源
    sc.stop()
  }

}
