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
4. 上个版本（Auto01）(0-8Direct自动维护offset)，只是指定了检查点，只会将offset放到检查点中，但是并没有从检查点中取，
    没有指定获取检查点的路径（只存不取），依然会存在消息丢失，需要改变创建StreamingContext方式。
5. 修改StreamingContext对象的获取方式:通过getActiveOrCreate（检查点路径，创建StreamingContext）
    因为消费者会从检查点中读取offset，所以就算程序停了，生产者继续生产消息，当消费者再次启动，也不会出现丢失数据的情况。
6. 自定维护offset：通过修改StreamingContext对象的获取方式：先从检查点获取，如果检查点没有，通过函数创建，会保证数据不丢失。
    1）优点：保证数据不丢失
    2）缺点：
      2.1）小文件过多
      2.2）在checkpoint中只记录最后offset的时间戳，再次启动程序的时候，会从这个时间到当前时间，把所有周期都执行一遍。
          比如批处理时间间隔为3秒，20:15:00最后一次消费，停了3分钟，再次开启时，会把3分钟这个时间间隔的批处理全部执行
          一遍60次，59次都是空的，如果停了一天，再开启，效率很低，存在问题。
 */
object Test06_DirectAPI_Auto02 {
  def main(args: Array[String]): Unit = {
    // 创建StreamingContext上下文环境对象;
    // getActiveOrCreate参数解读：1）检查点路径，如果检查点路径存在就从检查点中获取StreamingContext对象，
    // 2）空参函数：如果检查点中没有就创建StreamingContext对象，里面写着创建的逻辑，但是没有
//    val ssc:StreamingContext = StreamingContext.getActiveOrCreate("D:\\workspace_idea\\spark0621\\spark0621exer\\cp", () => getStreamingContext)
//
//    // 开启任务
//    ssc.start()
//    ssc.awaitTermination()

  }

//  def getStreamingContext: StreamingContext = {
//    //创建配置文件对象
//    val conf: SparkConf = new SparkConf().setAppName("china").setMaster("local[*]")
//
//    //创建SparkStreaming程序的入口，StreamingContext
//    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
//
//    //设置检查点目录
//    ssc.checkpoint("D:\\workspace_idea\\spark0621\\spark0621exer\\cp")
//
//    //准备kafka参数
//    val kafkaParams: Map[String, String] = Map[String, String](
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092", //指定kafka集群位置
//      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata" // 指定消费者组
//    )
//
//    //连接kafka,创建DStream
//    //createDirectStream泛型解读：1）topic消息的k类型；2）topic消息的v类型；3）k的反序列化方法；4）v的反序列化方法
//    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc,
//      kafkaParams,
//      Set("test")
//    )
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
//   //将ssc对象返回
//    ssc
//  }

}
