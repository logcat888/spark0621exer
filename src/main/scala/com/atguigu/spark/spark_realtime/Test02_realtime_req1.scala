package com.atguigu.spark.spark_realtime

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @author chenhuiup
 * @create 2020-10-02 21:41
 */
/*
需求：每天每地区热门广告top3
 */
object Test02_realtime_req1 {
  def main(args: Array[String]): Unit = {
    // 0.创建配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("China")
    // 1.创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(6))

    // 6.设置检查点路径
    ssc.sparkContext.setCheckpointDir("D:\\workspace_idea\\spark0621\\spark0621exer\\output1")

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

    // 4.1 获取消息的value
    val windowDS: DStream[String] = kafkaDStream.map(_.value())
    // (day-area-ad,1)
    val dayAndAreaAndAdToOne: DStream[(String, Int)] = windowDS.map { info => {
      val line = info.split(",")
      // 将时间戳转化为Date类
      val date = new Date(line(0).toLong)
      // 将Date类格式化为2020-07-12
      val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
      // (day-area-ad,1)
      (dateString + "&" + line(1) + "&" + line(4), 1)
    }
    }
    // 对每天没地去广告点击数进行聚合处理  (day-area-ad,n)
    // 注意：这里要统计的是一天的数据，所以要将每一个采集周期的数据都统计，需要传递状态，所以要用updateStateByKey
    val resDS: DStream[(String, Int)] = dayAndAreaAndAdToOne.updateStateByKey(
      (seq: Seq[Int], state: Option[Int]) => {
        Option(seq.sum + state.getOrElse(0))
      }
    )
    // (day-erea,(ad,n))
    val mapDS: DStream[(String, (String, Int))] = resDS.map({
      case (data, count) => {
        val line = data.split("&")
        (line(0) + "-" + line(1), (line(2), count))
      }
    })
    // 分组排序,排序，取前三
    val value: DStream[(String, List[(String, Int)])] = mapDS.groupByKey().mapValues(data => data.toList.sortBy(-_._2).take(3))
    //打印输出
    value.print()


//
//    val mapDS: DStream[(String, String, String, Int)] = resDS.map {
//      case (data, count) => {
//        val line = data.split("-")
//        (line(0), line(1), line(2), count)
//      }
//    }
//    mapDS.foreachRDD(
//      rdd =>{
//        val df = rdd.toDF("day","area","ad","count")
//        df.createOrReplaceTempView("t1")
//        spark.sql("select day,area,ad,count, rank() over(partition by day,area order by count desc ) new_rank from t1")
//          .createOrReplaceTempView("t2")
//        spark.sql("select day,area,ad,count,new_rank from t2 where new_rank < 4").show()
//      }
//    )

    // 5.开始任务
    ssc.start()
    ssc.awaitTermination()
  }
}
