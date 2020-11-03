package com.atguigu.spark.sparkstreaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @author chenhuiup
 * @create 2020-10-02 18:49
 */
/*
优雅的关闭：思路是使用标记位
https://blog.csdn.net/zwgdft/article/details/85849153
 */
object Test12_gracefulStop {
  def main(args: Array[String]): Unit = {

    //创建配置文件对象
    //注意：Streaming程序执行至少需要2个线程，一个用于接收器采集数据，一个用于处理数据，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("China").setMaster("local[*]")

    //设置优雅的关闭
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）,指定 1)配置文件，以及 2)数据采集周期
    // 采集周期Duration，构造器默认单位是毫秒，可以通过伴生对象Seconds/Minutes方便指定采集周期
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //DStream也是抽象的弹性分布式数据集，放的是采集周期采集到数据，随着时间推移收到的数据序列，每个采集周期都是一个DStreams，
    // 在内部，每个时间区间收到的数据都作为RDD存在，相当于是对RDD的封装，所以DStreams与RDD、DataFrame、DataSet都是一样的。
    //扁平化
    val flatMap: DStream[String] = socketDS.flatMap(_.split(" "))

    // 结构转换，进行计数
    val mapDS: DStream[(String, Int)] = flatMap.map((_,1))

    // 聚合操作
    val resDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    // 打印输出  print方法默认值打印数据集中的前10条
    // def print(): Unit = ssc.withScope {print(10)}
    resDS.print()

    //----------------------- 优雅的关闭---------------------

    // 启动新的线程，希望在特殊的场合关闭SparkStreaming
    new Thread(new Runnable {
      override def run(): Unit = {

        while ( true ) {
          try {
            Thread.sleep(5000)
          } catch {
            case ex : Exception => println(ex)
          }

          // 监控HDFS文件的变化
          val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop202:8020"), new Configuration(), "atguigu")

          val state: StreamingContextState = ssc.getState()
          // 如果环境对象处于活动状态，可以进行关闭操作
          if ( state == StreamingContextState.ACTIVE ) {

            // 判断路径是否存在，当hdfs多出这个目录，就停止线程，关闭sparkcontext上下文对象，然后优雅的关闭
            val flg: Boolean = fs.exists(new Path("hdfs://hadoop202:8020/stopSpark2"))
            if ( flg ) {
              // 第一个参数是关闭SparkContext上下文对象，第二个参数是是否优雅的关闭。当优雅的关闭时会对资源进行合理的安排，
              // 而不是半路终止。
              ssc.stop(true, true)
              System.exit(0)
            }

          }
        }

      }
    }).start()

    //启动采集器。 采集器只是一个守护线程，放在print后面可以。
    ssc.start()

    //等到采集结束之后，终止程序
    ssc.awaitTermination()
  }

}
