package com.atguigu.spark.sparkstreaming

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chenhuiup
 * @create 2020-10-02 10:48
 */
/*
通过DirectAPi 0-10 消费kafka数据
1. 消费的offset自动保存在kafka集群的__consumer_offsets主题中，不像0-8保存在zookeeper（效率低）或者检查点中（小文件多，重复执行）
 */
object Test08_DirectAPI_010 {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("China")
    //创建SparkStreaming程序的入口，创建StreamingContext对象,传入参数配置信息，以及采集周期
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //定义kafka相关的连接参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      // 指定kafka集群位置
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      // 指定消费者组名
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
      // 指定消息的key的反序列化，全类名，通过反射获取反序列化器
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      // 指定消息的value的反序列化，使用classof方法获取全类名，通过反射获取反序列化器
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //通过kafkaUtils创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      // def createDirectStream[K, V] 需要指定泛型，否则是Any类型
      ssc, // StreamingContext对象
      //位置策略，指定计算的Executor，PreferConsistent将kafka分区交给所有的Executor节点
      LocationStrategies.PreferConsistent,
      //消费策略，参数解读：1）可迭代的集合内放入topic；2）kafka连接相关参数；3）offset
      ConsumerStrategies.Subscribe[String, String](Set("test2"), kafkaParams)
      // def Subscribe[K, V] 需要指定泛型，否则是Any类型,KV代表kafka消息的kv类型
    )

    // 获取消息的value
    val valueDS: DStream[String] = kafkaDStream.map(_.value())

    //wordcount
    val resDS: DStream[(String, Int)] = valueDS.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //打印
    resDS.print()

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}


/*
 位置策略
 * Object to obtain instances of [[LocationStrategy]]
object LocationStrategies {

   * Use this only if your executors are on the same nodes as your Kafka brokers.

   当处理数据的Executor与消费的kafka分区在同一个节点上使用，这种数据就在同一个节点上，效率最高。
   但是大多数情况下，kafka与yarn的Executor不在同一节点比如kafka在（hadoop102、hadoop103，hadoop104），
   而Executor在其他节点如hadoop105等，这种情况下就不适合。
  def PreferBrokers: LocationStrategy =
    org.apache.spark.streaming.kafka010.PreferBrokers


   * Use this in most cases, it will consistently distribute partitions across all executors.

   // 在大多数情况下的位置策略，一个Executor对应一个kafka中topic中的一个分区。
  def PreferConsistent: LocationStrategy =
    org.apache.spark.streaming.kafka010.PreferConsistent


   * Use this to place particular TopicPartitions on particular hosts if your load is uneven.
   * Any TopicPartition not specified in the map will use a consistent location.

   固定Executor的位置，Map[TopicPartition, String]为将指定topic的分区交给指定节点处理，
   在map集合中可以指定多个主题分区对应指定节点，这样就比较死，不灵活，很少用。
   比如 topicA中的0号分区交给 hadoop102执行，topicA中的1号分区交给 hadoop103执行.

  def PreferFixed(hostMap: collection.Map[TopicPartition, String]): LocationStrategy =
    new PreferFixed(new ju.HashMap[TopicPartition, String](hostMap.asJava))


   * Use this to place particular TopicPartitions on particular hosts if your load is uneven.
   * Any TopicPartition not specified in the map will use a consistent location.

  def PreferFixed(hostMap: ju.Map[TopicPartition, String]): LocationStrategy =
    new PreferFixed(hostMap)
}
*/