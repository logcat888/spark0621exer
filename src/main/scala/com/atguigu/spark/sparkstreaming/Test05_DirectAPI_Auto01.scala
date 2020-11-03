package com.atguigu.spark.sparkstreaming

import org.apache.commons.codec.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chenhuiup
 * @create 2020-10-01 21:34
 */
/*
0-8:DirectAPI在spark2.3以后就过时了，目前3.0.0无法使用
通过DirectAPI连接Kafka数据源，获取数据
1. 自动的维护偏移量offset，偏移量维护在checkpoint中
2. 如果检查点目录没有设置，重新启动消费者，会无法消费未消费的数据，只能从最新的消息开始消费，数据丢失。
3. 因为ssc中包含SparkContext属性，所以可以直接通过ssc设置检查点
4. 目前我们这个版本(0-8Direct自动维护offset)，只是指定了检查点，只会将offset放到检查点中，但是并没有从检查点中取，
    没有指定获取检查点的路径（只存不取），依然会存在消息丢失，需要改变创建StreamingContext方式。
 */
object Test05_DirectAPI_Auto01 {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("china").setMaster("local[*]")

    //创建SparkStreaming程序的入口，StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //设置检查点目录
    ssc.checkpoint("D:\\workspace_idea\\spark0621\\spark0621exer\\cp")

    //准备kafka参数
    val kafkaParams: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092", //指定kafka集群位置
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata" // 指定消费者组
    )


    //连接kafka,创建DStream
    //createDirectStream泛型解读：1）topic消息的k类型；2）topic消息的v类型；3）k的反序列化方法；4）v的反序列化方法
    //    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    //      ssc,
    //      kafkaParams,
    //      Set("test")
    //    )
    //
    //
    //    //获取kafka中的消息，我们只需要v的部分
    //    val line06: DStream[String] = kafkaDStream.map(_._2).
    //
    //    //扁平化处理
    //    //扁平化
    //    val flatMap: DStream[String] = line06.flatMap(_.split(" "))
    //
    //    // 结构转换，进行计数
    //    val mapDS: DStream[(String, Int)] = flatMap.map((_, 1))
    //
    //    // 聚合操作
    //    val resDS: DStream[(String, Int)] = mapDS.reduceByKey(_ + _)
    //
    //    //打印输出
    //    resDS.print()
    //
    //    //开启采集器
    //    ssc.start()
    //
    //    //等待采集结束，终止程序
    //    ssc.awaitTermination()
    //  }
  }
}
