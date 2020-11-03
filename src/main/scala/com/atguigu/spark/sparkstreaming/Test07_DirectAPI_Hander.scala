package com.atguigu.spark.sparkstreaming

import org.apache.commons.codec.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
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
针对自动维护offset的问题 升级为手动维护offset
7. 手动维护offset
1）准备kafka参数：1.1）kafka集群位置，1.2）消费者组
2）通过kafkaUtils.createDirectStream创建DStream：参数解读
    2.1）StreamingContext对象
    2.2) kafka参数
    2.3) 获取上一次消费的位置Map[TopicAndPartition,Long]，通过定义返回这个集合
    2.4) 传递一个函数 (m:MessageAndMetadata[String,String]) => m.message()
    2.5) createDirectStream泛型解读：1）topic消息的k类型；2）topic消息的v类型；3）k的反序列化方法；4）v的反序列化方法；5）返回类型
3) 定义一个方法，获取上一次消费的位置（偏移量），返回值为Map[TopicAndPartition,Long]:
    Map[TopicAndPartition,Long]参数解读：
    3.1）TopicAndPartition，获取topic和分区
    3.2）Long为offset
    3.3）偏移量的保存做成事务保存在mysql中，通过访问mysql数据库获取topic、分区、偏移量
4）消费完毕之后，对偏移量offset进行更新：
    4.1）获取offsetRanges保存到数组中，每一个rdd都是有序的
    4.2）遍历rdd从offset数组中将offset写入到数据库等
8. 如何做到精准一次性消费：
  实际项目中，为了保证数据精准一次性，我们对数据进行消费处理之后，将偏移量保存在有事务的存储中，如MySQL。
  偏移量和数据保持一致，比如对kafka中的数据进行消费，现在的处理方式是只要读到数据就认为偏移量更改了，但是在实际项目中，获取到
    消息，当消息处理成功后才认为是数据被消费了，才更改偏移量，所以把消息处理成功和偏移量做成一个事务，如果都成功再把偏移量写入到
    mysql数据库中，如果没有成功做回滚。他们两是原子性操作，要么都成功，要么都失败。比如从kafka获取数据，消费效果
    没有达到预期，这时偏移量不会更改，当下一次消费时还是从之前的偏移量开始重新消费。
9. 手动更新offset：
    DStream离散化流是一个抽象的概念，是对RDD的封装，RDD也是一个抽象的概念，RDD有很多实现，我们需要是KafkaRDD。
    所以通过DStream中的RDD进行强制类型转换为KafkaRDD,但是KafkaRDD是私有的，只有KafkaRDD中才有分区信息，offset信息，
    需要找到他的特质,通过多态使用子类对象的方法offsetRanges。
 */
object Test07_DirectAPI_Hander {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("china").setMaster("local[*]")

    //创建SparkStreaming程序的入口，StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))


//    //准备kafka参数
//    val kafkaParams: Map[String, String] = Map[String, String](
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092", //指定kafka集群位置
//      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata" // 指定消费者组
//    )
//
//    //获取上一次消费的位置（偏移量）
//    //实际项目中，为了保证数据精准一次性，我们对数据进行消费处理之后，将偏移量保存在有事务的存储中，如MySQL。
//    /*
//    偏移量和数据保持一致，比如对kafka中的数据进行消费，现在的处理方式是只要读到数据就认为偏移量更改了，但是在实际项目中，获取到
//    消息，当消息处理成功后才认为是数据被消费了，才更改偏移量，所以把消息处理成功和偏移量做成一个事务，如果都成功再把偏移量写入到
//    mysql数据库中，如果没有成功做回滚。他们两是原子性操作，要么都成功，要么都失败。比如从kafka获取数据，消费效果
//    没有达到预期，这时偏移量不会更改，当下一次消费时还是从之前的偏移量开始重新消费。
//     */
//    // Map[TopicAndPartition,Long] 泛型参数解读：TopicAndPartition为主题和分区，Long为上次消费的offset值
//    def fromOffset():Map[TopicAndPartition,Long] ={
//      Map[TopicAndPartition,Long](
//        TopicAndPartition("bigdata",0) -> 1L, //指定offset消费，从数据库中获取offset，这里随便写的1，transform方法更新offset
//        TopicAndPartition("bigdata",1) -> 5L //指定offset消费，从数据库中获取offset，这里随便写的1
//      )
//    }
//
//
//    //连接kafka,创建DStream,从指定的offset读取数据进行消费
//    //createDirectStream泛型解读：1）topic消息的k类型；2）topic消息的v类型；3）k的反序列化方法；4）v的反序列化方法；5）返回类型
//    val kafkaDStream: InputDStream[ String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,String](
//      ssc, //StreamingContext对象
//      kafkaParams, //kafka参数
//      fromOffset, // 手动设置offset，定义一个方法获取offset，以便知道从什么位置消费
//      (m:MessageAndMetadata[String,String]) => m.message()
//    )
//
//    //消费完毕之后，对偏移量offset进行更新
//    // DStream离散化流是一个抽象的概念，是对RDD的封装，RDD也是一个抽象的概念，RDD有很多实现，我们需要是KafkaRDD。
//    //  所以通过DStream中的RDD进行强制类型转换为KafkaRDD,但是KafkaRDD是私有的，只有KafkaRDD中才有分区信息，offset信息，
//    // 需要找到他的特质,通过多态使用子类对象的方法offsetRanges。
//    //定义一个空集合，接收OffsetRange
//    val offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
//    kafkaDStream.transform{
//      rdd => {
//        //用空集合接收OffsetRange，获取rdd偏移量范围
//        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//        rdd  // 只是获取偏移量范围，不对rdd操作，所以返回rdd
//      }
//    }.foreachRDD{
//      rdd => {
//        //循环遍历每一个RDD中可以获取offset的位置，同时也可以获取topic/partition信息
//        for (o <- offsetRanges){
//          // 获取到offset的位置，可以保存到mysql数据库，这里只是简单的打印出来
//          // 其实可以把untilOffset保存到mysql中，下次在fromOffset方法中就可以通过访问mysql获取offset
//          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//        }
//      }
//    }
//
//    //获取kafka中的消息，我们只需要v的部分
//    val line06: DStream[String] = kafkaDStream.map(_._2)
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
//    // 开启任务
//    ssc.start()
//    ssc.awaitTermination()
  }
}
