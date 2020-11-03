package com.atguigu.spark.spark_realtime

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chenhuiup
 * @create 2020-10-02 21:44
 */
/*
需求：统计各广告最近1小时内的点击量趋势，每6s更新一次（各广告最近1小时内各分钟的点击量）
从kafka消费数据
 */
object Test03_realtime_req2 {
  def main(args: Array[String]): Unit = {
    // 0.创建配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("China")
    // 1.创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(6))

    // 3.创建kafka连接参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata2",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    // 2.创建kafka数据源的DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("test2"), kafkaParams)
    )

    // 5. 创建SparkSession对象，导包，以便可以隐式转换
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 导包以便rdd能够调用toDS算子
    import spark.implicits._

    // 4.将DStream转换为DataFrame，使用SparkSQL

    // 4.1 开窗
    val windowDS: DStream[String] = kafkaDStream.map(_.value()).window(Seconds(6 * 10 * 60))
    //
    windowDS.foreachRDD(
      rdd => {
        val reduceRDD = rdd.map {
          infos => {
            val info = infos.split(",")
            // 将时间戳转化为Date类
            val date = new Date(info(0).toLong)
            // 将Date类格式化为2020-07-12 02:07:59
            val dateString: String = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(date)
            val hour: Array[String] = dateString.split(" ")
            // 提取 (时间&&广告,1)
            val time1 =  hour(1).split(":")
            ((time1(0)+":"+time1(1) + "&&" + info(4)), 1)
          }
        }.reduceByKey(_ + _)
        // 变换结构为（时间，广告，n）,获取df
        val df: DataFrame = reduceRDD.map {
          case (timeAndAd, count) => {
            val data = timeAndAd.split("&&")
            (data(0), data(1), count)
          }
        }.toDF("time", "adsId", "count")
        // 创建临时视图
        df.createOrReplaceTempView("t1")

        spark.sql("select * from t1 order by time,count ").show()

      }
    )


    // 5.开始任务
    ssc.start()
    ssc.awaitTermination()

  }
}
