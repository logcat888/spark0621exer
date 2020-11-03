package com.atguigu.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chenhuiup
 * @create 2020-10-02 15:32
 */
/*
有状态转换--updateStateByKey
1. 有状态需要将状态保存在checkpoint路径中。
2. 有状态是会将状态一直延续下去，比如之前的状态是(a,5),当前批次输入 a a ，则累加之前的状态输出(a,7),
    如果一直不向端口发送数据,则一直打印(a,7)
3. reduceByKey是无状态的，只会对当前采集周期的数据进行聚合操作
4. updateStateByKey参数解读：
  1）Seq[V]：表示的是相同的key对应的value组成的数据集合
  2） Option[S]：表示的是相同的key的缓冲区数据
  def updateStateByKey[S: ClassTag](
      updateFunc: (Seq[V], Option[S]) => Option[S]
    ): DStream[(K, S)]
 */
object Test10_updateStateByKey {
  def main(args: Array[String]): Unit = {

    //创建配置文件对象
    //注意：Streaming程序执行至少需要2个线程，一个用于接收器采集数据，一个用于处理数据，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("China").setMaster("local[*]")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）,指定 1)配置文件，以及 2)数据采集周期
    // 采集周期Duration，构造器默认单位是毫秒，可以通过伴生对象Seconds/Minutes方便指定采集周期
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //设置检查点路径，状态保存在checkpoint中
    ssc.checkpoint("D:\\workspace_idea\\spark0621\\spark0621exer\\cp")

    // 从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //扁平化
    val flatMap: DStream[String] = socketDS.flatMap(_.split(" "))

    // 结构转换，进行计数
    val mapDS: DStream[(String, Int)] = flatMap.map((_,1))

    // 聚合操作   reduceByKey是无状态的，只会对当前采集周期的数据进行聚合操作
//    val resDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    // 打印输出  print方法默认值打印数据集中的前10条
//    resDS.print()

    /*
    数据（hello，1），（hello，1），（hello，1） ==>  (1,1,1)
     */
    val resRDD: DStream[(String, Int)] = mapDS.updateStateByKey(
      // 第一个参数：表示的是相同的key对应的value组成的数据集合
      // 第二个参数：表示的是相同的key的缓冲区数据
      (seq: Seq[Int], state: Option[Int]) => {
        // 对当前key对应的value进行求和
        //seq.sum
        //获取缓冲区数据,有可能缓冲区中没有数据，为了防止空指针封装成Option对象，当缓冲区中没有值时返回0
        //state.getOrElse(0)
        Option(seq.sum + state.getOrElse(0))
      }
    )
    resRDD.print()

    //

    //启动采集器。 采集器只是一个守护线程，放在print后面可以。
    ssc.start()

    //等到采集结束之后，终止程序
    ssc.awaitTermination()
  }
}
