package com.atguigu.spark.sparkstreaming

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author chenhuiup
 * @create 2020-10-01 16:39
 */
/*
通过RDD队列方式创建DStream：数据来源于RDD队列，使用用于模拟流式数据，使用情况不多，用于测试
需求：循环创建几个RDD，将RDD放入队列。通过SparkStream创建Dstream，计算WordCount
1. 代码的运行过程：
    1） 创建空队列，从空队列中采集数据，对采集的数据进行结构变换聚合，再打印输出，接下来采集器启动。
    2） 在采集器启动时采集不到数据，因为队列里没有数据，但是没有关系，因为当我们启动采集器时，相当于开了一个线程去执行采集数据。
    3） 接下来程序继续向下执行，向队列中放入RDD，然后每隔3秒中采集器向队列中采集一波数据，这样采集器内就有数据了，就可以进行数据处理，打印输出。
    4） 所以别看for循环在start的下面，其实start底层是开了一个线程去执行采集数据，接下来采集完数据之后，对源源不断的数据进行处理，
        ssc.queueStream(rddQueue)代码并没有执行完毕，是每隔3秒对源源不断的数据进行处理。
2. 程序运行时，至少有3个线程，主线程，采集线程，执行器线程
3. 从队列中采集数据，获取DS ： 默认在一个采集周期从队列中采集一个RDD，如果oneAtATime设置为false有可能一个采集周期获取多个RDD
  queueStream参数解读：1）队列；2）是否一次只取一个RDD
4.
 */
object Test02_RDDQueue {
  def main(args: Array[String]): Unit = {

    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("China")

    //创建SparkStreaming上下文环境对象，StreamingContext对象，指定配置文件和批次处理时间
    val ssc = new StreamingContext(conf,Seconds(3))

    //创建队列，里面放的是RDD
    val rddQueue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()

    //从队列中采集数据，获取DS ： 默认在一个采集周期从队列中采集一个RDD，如果oneAtATime设置为false有可能一个采集周期获取多个RDD
    // queueStream参数解读：1）队列；2）是否一次只取一个RDD
    val queueDS: InputDStream[Int] = ssc.queueStream(rddQueue)
    /*
    def queueStream[T: ClassTag](
      queue: Queue[RDD[T]],
      oneAtATime: Boolean = true
    ): InputDStream[T] = {
    queueStream(queue, oneAtATime, sc.makeRDD(Seq.empty[T], 1))
  }
     */

    //处理采集到的数据
    val resDS: DStream[(Int, Int)] = queueDS.map((_,1)).reduceByKey(_+_)

    //打印结果
    // 程序运行时，至少有3个线程，主线程，采集线程，执行器线程
    resDS.print()

    // 启动采集器： 相当于开了一个线程去执行采集数据
    ssc.start()
    /*
    代码的运行过程：
    1. 创建空队列，从空队列中采集数据，对采集的数据进行结构变换聚合，再打印输出，接下来采集器启动。
    2. 在采集器启动时采集不到数据，因为队列里没有数据，但是没有关系，因为当我们启动采集器时，相当于开了一个线程去执行采集数据。
    3. 接下来程序继续向下执行，向队列中放入RDD，然后每隔3秒中采集器向队列中采集一波数据，这样采集器内就有数据了，就可以进行数据处理，打印输出。
    4. 所以别看for循环在start的下面，其实start底层是开了一个线程去执行采集数据，接下来采集完数据之后，对源源不断的数据进行处理，
        ssc.queueStream(rddQueue)代码并没有执行完毕，是每隔3秒对源源不断的数据进行处理。
     */

    //循环创建RDD，并将创建的RDD放到队列里
    for (i <- 1 to 5){
      rddQueue.enqueue(ssc.sparkContext.makeRDD(6 to 10))
      // 创建RDD时让线程睡2秒
      TimeUnit.SECONDS.sleep(2)
    }

    //等待采集结束再去停止程序
    ssc.awaitTermination()

//    -------------------------------------------
//    Time: 1601544192000 ms     0秒
//      -------------------------------------------
//    (6,1)
//    (7,1)
//    (8,1)
//    (9,1)
//    (10,1)
//
//    -------------------------------------------
//    Time: 1601544195000 ms        3秒
//      -------------------------------------------
//    (6,1)
//    (7,1)
//    (8,1)
//    (9,1)
//    (10,1)
//
//    -------------------------------------------
//    Time: 1601544198000 ms        6秒
//      -------------------------------------------
//    (6,1)
//    (7,1)
//    (8,1)
//    (9,1)
//    (10,1)
//
//    -------------------------------------------
//    Time: 1601544201000 ms        9秒
//      -------------------------------------------
//    (6,1)
//    (7,1)
//    (8,1)
//    (9,1)
//    (10,1)
//
//    -------------------------------------------
//    Time: 1601544204000 ms        12秒
//      -------------------------------------------
//    (6,1)
//    (7,1)
//    (8,1)
//    (9,1)
//    (10,1)
//
//    -------------------------------------------
//    Time: 1601544207000 ms        15秒
//      -------------------------------------------
//
//    -------------------------------------------
//    Time: 1601544210000 ms        18秒
//      -------------------------------------------
//
//          ..............   继续从队列中采集数据，直接手动停止

  }

}
/*
数据可以来源于mysql，hdfs，比如设置一个定时任务，每隔一定时间从mysql中获取数据，当做源源不断的数据源，通过自定义数据源创建离散化流。
 */