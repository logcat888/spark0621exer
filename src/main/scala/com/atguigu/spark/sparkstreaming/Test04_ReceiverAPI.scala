package com.atguigu.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chenhuiup
 * @create 2020-10-01 20:54
 */
/*
0-8 通过ReceiverAPI连接kafka数据源，获取数据（已经过时不用了）
 */
object Test04_ReceiverAPI {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("china").setMaster("local[*]")

    //创建SparkStreaming程序的入口，StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

//    //连接kafka,创建DStream
//   val kafkaDStream :ReceiverInputDStream[(String,String)] = KafkaUtils.createStream(
//      ssc, //StreamingContext
//      "hadoop102:2181,hadoop103:2181,hadoop104:2181", //zookeeper集群
//      "bigdata", // 消费者组
//      Map("bigdata-0105"->2) //topic 及分区数
//    )
//
//    //获取kafka中的消息，我们只需要v的部分
//    val line06:DStream[String] = kafkaDStream.map(_._2).var
//
//    //扁平化处理
//    //扁平化
//    val flatMap: DStream[String] = line06.flatMap(_.split(" "))
//
//    // 结构转换，进行计数
//    val mapDS: DStream[(String, Int)] = flatMap.map((_,1))
//
//    // 聚合操作
//    val resDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)
//
//    //打印输出
//    resDS.print()
//
//    //开启采集器
//    ssc.start()
//
//    //等待采集结束，终止程序
//    ssc.awaitTermination()
  }



}
