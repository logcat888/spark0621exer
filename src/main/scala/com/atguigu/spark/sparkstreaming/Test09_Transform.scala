package com.atguigu.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @author chenhuiup
 * @create 2020-10-02 15:32
 */
/*
使用transform算子将DStream转换为rdd
1. DStream中没有sort算子,通过transform转换为RDD，在DStream内部进行RDD的操作，这样就可以使用RDD的算子
 */
object Test09_Transform {
  def main(args: Array[String]): Unit = {

    //创建配置文件对象
    //注意：Streaming程序执行至少需要2个线程，一个用于接收器采集数据，一个用于处理数据，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("China").setMaster("local[*]")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）,指定 1)配置文件，以及 2)数据采集周期
    // 采集周期Duration，构造器默认单位是毫秒，可以通过伴生对象Seconds/Minutes方便指定采集周期
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    // 从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    // 需求：实现每个采集周期内的排序
    //将DS转换为RDD进行操作
    val resDS: DStream[(String, Int)] = socketDS.transform(
      //这个rdd就是DStream底层封装的RDD
      rdd => {
        //使用RDD相关的算子
        val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        //返回RDD
        reduceRDD.sortByKey()
      }
      // def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]
    )
    //打印输出
    resDS.print()

    /*
    //扁平化
    val flatMap: DStream[String] = socketDS.flatMap(_.split(" "))

    // 结构转换，进行计数
    val mapDS: DStream[(String, Int)] = flatMap.map((_,1))

    // 聚合操作
    val resDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    // 打印输出  print方法默认值打印数据集中的前10条
    resDS.print()
     */

    //启动采集器。 采集器只是一个守护线程，放在print后面可以。
    ssc.start()

    //等到采集结束之后，终止程序
    ssc.awaitTermination()
  }
}
