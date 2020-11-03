package com.atguigu.spark.spark_realtime

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chenhuiup
 * @create 2020-10-02 21:12
 */
/*
实时消费kafka中的数据
 */
object Test01_ConsumerRealtime {
  def main(args: Array[String]): Unit = {
    // 创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("China").setMaster("local[*]")
    // 创建SparkStreaming程序的入口，StreamingContext对象,传入配置文件对象和采集周期
    val ssc = new StreamingContext(conf,Seconds(3))

    //创建kafka连接参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      //指定kafka集群位置
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      //指定消费者组
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata2",
      //指定key的反序列化方法
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      //指定value的反序列化方法
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    // 创建kafka数据源的DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc, //StreamingContext对象
      //位置策略
      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe[String, String](Set("test2"), kafkaParams)
    )

    // 对DStream操作
    val resDS: DStream[String] = kafkaDStream.map(_.value())
    resDS.print()

    // 开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
