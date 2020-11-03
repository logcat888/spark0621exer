package com.atguigu.spark.sparkstreaming

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * @author chenhuiup
 * @create 2020-10-01 16:39
 */
/*
通过自定义数据源方式，创建DStream。  模拟从指定的网络端口获取数据源
1. 目前的数据源： 1）queueStream，队列；2）socketTextStream，网络端口
2. 自定义数据源的步骤：
  1）继承Receiver：指定采集的泛型以及存储级别
  2）实现两个onStop（释放资源）/onStart（创建守护线程采集数据，在ssc.start时被调用）方法的固定写法
  3）实现核心业务逻辑receive方法，将采集的数据放入store方法中缓存，形成一批次数据
3. 通过自定义数据源创建DStream。调用ssc的receiveStream方法
 */
object Test03_CustomerReceiver {
  def main(args: Array[String]): Unit = {

    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("China")

    //创建SparkStreaming上下文环境对象，StreamingContext对象，指定配置文件和批次处理时间
    val ssc = new StreamingContext(conf, Seconds(3))

    //通过自定义数据源创建DStream
    val myDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceive("hadoop102",7777))

    //扁平化
    val flatMap: DStream[String] = myDS.flatMap(_.split(" "))

    // 结构转换，进行计数
    val mapDS: DStream[(String, Int)] = flatMap.map((_,1))

    // 聚合操作
    val resDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    //打印输出
    resDS.print()

    //开启采集器
    ssc.start()

    //等待采集结束，终止程序
    ssc.awaitTermination()
  }

}

/*
模拟从指定的网络端口获取数据源：
Receiver[T] ：T为采集数据的泛型
StorageLevel.MEMORY_ONLY： 指定存储级别
 */
class MyReceive(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  private var socket: Socket = _ //网络套接字

  //创建线程，固定写法
  override def onStart(): Unit = {

    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") { //创建一个线程，在scc.start() 时被调用，开始采集数据
      setDaemon(true) //将当前线程设置为守护线程
      // run方法，启动线程，调用receive()，完成接收数据,真正接收数据的逻辑在receive方法中
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  // 释放资源，固定写法
  override def onStop(): Unit = {
    synchronized {
      if (socket != null) {
        socket.close() // 释放资源
        socket = null
      }
    }
  }

  //真正的处理结束数据的逻辑：数据的来源可以不是一个端口，也可能是Redis，或从某个消息队列读取数据
  def receive(): Unit = { //真正接收数据的逻辑
    try { //网络编程：连接到指定端口获取数据
      try {
        // 创建连接
        socket = new Socket(host, port) // 创建一个socket对象
        // 根据连接对象获取输入流
        // 1)socket.getInputStream, socket只能获取字节流，从网络端口获取一行数据，所以需要转换包装
        // 2）InputStreamReader转换流，将字节转换为字符，需要指定字符集，可以使用“utf-8”，但是不稳定，
        // 使用静态属性获取public static final Charset UTF_8 = Charset.forName("UTF-8");，
        // 3） BufferedReader 缓冲流，有一个一次读一行的方法readline()，从而可以一次可以网络端口获取一行数据
       val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
        var input:String = null
        while ((input = reader.readLine()) != null){
          // 调用store方法将读到的数据缓存起来，作为一个批次的数据，具体如何实现采集周期数据的保存，框架自己写了
          store(input)
        }
      } catch {
        case e: ConnectException =>
          restart(s"Error connecting to $host:$port", e)
          return
      }

    } catch {
      case e: ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    } finally {
      onStop()
    }
  }
}


//class SocketReceiver[T: ClassTag](
//                                   host: String,
//                                   port: Int,
//                                   bytesToObjects: InputStream => Iterator[T],
//                                   storageLevel: StorageLevel //存储级别
//                                 ) extends Receiver[T](storageLevel) with Logging {
//
//  private var socket: Socket = _ //网络套接字
//
//  def onStart(): Unit = {
//
//    logInfo(s"Connecting to $host:$port")  // 打印日志
//    try {
//      socket = new Socket(host, port) // 创建一个socket对象
//    } catch {
//      case e: ConnectException =>
//        restart(s"Error connecting to $host:$port", e)
//        return
//    }
//    logInfo(s"Connected to $host:$port")
//
//    // Start the thread that receives data over a connection
//    new Thread("Socket Receiver") { //创建一个线程，在scc.start() 时被调用，开始采集数据
//      setDaemon(true) //将当前线程设置为守护线程
//      override def run(): Unit = { receive() } // run方法，启动线程，调用receive()，完成接收数据
//    }.start()
//  }
//
//  def onStop(): Unit = {
//    // in case restart thread close it twice
//    synchronized {
//      if (socket != null) {
//        socket.close()  // 释放资源
//        socket = null
//        logInfo(s"Closed socket to $host:$port")  //打印日志
//      }
//    }
//  }
/** Create a socket connection and receive data until receiver is stopped */
//    def receive(): Unit = { //真正接收数据的逻辑
//      try {
//      val iterator = bytesToObjects(socket.getInputStream())
//      while(!isStopped && iterator.hasNext) {
//      store(iterator.next())
//    }
//      if (!isStopped()) {
//      restart("Socket data stream had no more data")
//    } else {
//      logInfo("Stopped receiving")
//    }
//    } catch {
//      case NonFatal(e) =>
//      logWarning("Error receiving data", e)
//      restart("Error receiving data", e)
//    } finally {
//      onStop()
//    }
//    }
//    }