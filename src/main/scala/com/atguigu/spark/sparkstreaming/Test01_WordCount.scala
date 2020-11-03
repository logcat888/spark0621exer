package com.atguigu.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * @author chenhuiup
 * @create 2020-10-01 14:30
 */
/*
总结：
1. 获取程序执行的入口，上下文环境对象：StreamingContext对象，需要指定 1)配置文件，2)数据采集周期
2. 对数据集的抽象和封装为DStream
3. 启动采集器：需要源源不断的采集数据，ssc.start
4. 等待采集结束之后，终止程序：ssc.awaitTerminated。 必须让主线程阻塞，且不能直接终止采集器，所以等待结束
5. 开启端口输入数据netcat服务：  在shell窗口输入 nc -lk 9999 ，之后就可以一直输入消息
6.  Streaming程序执行至少需要2个线程，一个用于接收器采集数据，一个用于处理数据，所以不能设置为local
7. 从端口号中获取的数据是一行一行的，虽然在一个采集周期内采集数据封装到DStreams中，但是底层还是通过RDD对一行行数据进行处理，
    所以对DStream的处理本质还是对RDD的处理
8. DStream也是抽象的弹性分布式数据集，放的是采集周期采集到数据，随着时间推移收到的数据序列，每个采集周期都是一个DStreams，
    在内部，每个时间区间收到的数据都作为RDD存在，相当于是对RDD的封装，所以DStreams与RDD、DataFrame、DataSet都是一样的。
9. print方法默认值打印数据集中的前10条
10. 关于采集器启动、stop、awaitTerminated、print、主线程之间的关系：
    对于SparkStreaming需要至少2个线程，一个用于采集数据，其他线程用于对采集后的数据进行处理。
    1） 启动采集器：当没有启动采集器时，就打印输出，就相当于还没有开始采集数据，print就已经执行完了，现在就是太快了，还没有开始就结束了。
      所以看不到打印效果，如果想看到采集效果，就必须启动采集器。
    2） 如果只启动采集器，只会采集到一条数据，因为主线程已经结束，需要让主线程阻塞，这样采集器才能源源不断的采集数据，而不是采集一次就关闭。
    3） 让主线程一直阻塞不好，程序需要结束，所以数据采集完毕后，需要关闭采集器。但是不能调用stop方法去停止采集器，
      因为采集器需要源源不断的采集数据，默认情况下采集器不能关闭，只要调用stop方法，一个周期采集完后就结束程序了。
    4） 如果不关闭采集器，但是主程序已经执行完毕，采集器也躲不掉被关闭的命运。所以需要等到采集结束之后，再终止程序，其实就相当于不关。
      所以不能调用stop，也不能让主线程一直阻塞。这时就需要使用awaitTermination等待采集结束，去终止程序。
    5） 最终产生的效果就是，根据采集周期来获取数据，形成wordcount，源源不断的采集端口的数据，每3秒处理一次进行wordcount。

 */
object Test01_WordCount {
  def main(args: Array[String]): Unit = {

    //创建配置文件对象
    //注意：Streaming程序执行至少需要2个线程，一个用于接收器采集数据，一个用于处理数据，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("China").setMaster("local[*]")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）,指定 1)配置文件，以及 2)数据采集周期
    // 采集周期Duration，构造器默认单位是毫秒，可以通过伴生对象Seconds/Minutes方便指定采集周期
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    // 从指定的端口获取数据
    // 1. socketTextStream参数解读：第一个参数为主机名；第二个参数为端口号；第三个参数为存储级别，默认是MEMORY_AND_DISK_SER_2，默认是内存和磁盘，且有2个副本
    // 2. ReceiverInputDStream：是SparkStreaming中的离散化流，继承于DStreams，底层还是RDD
    // 3. 从端口号中获取的数据是一行一行的，虽然在一个采集周期内采集数据封装到DStreams中，但是底层还是通过RDD对一行行数据进行处理
    // abstract class ReceiverInputDStream[T: ClassTag](_ssc: StreamingContext) extends InputDStream[T](_ssc)
    // abstract class InputDStream[T: ClassTag](_ssc: StreamingContext) extends DStream[T](_ssc)
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

    /*
    如果打印从指定端口获取的数据：
    对于SparkStreaming需要至少2个线程，一个用于采集数据，其他线程用于对采集后的数据进行处理。
    1. 启动采集器：当没有启动采集器时，就打印输出，就相当于还没有开始采集数据，print就已经执行完了，现在就是太快了，还没有开始就结束了。
      所以看不到打印效果，如果想看到采集效果，就必须启动采集器。
    2. 如果只启动采集器，只会采集到一条数据，因为主线程已经结束，需要让主线程阻塞，这样采集器才能源源不断的采集数据，而不是采集一次就关闭。
    3. 让主线程一直阻塞不好，程序需要结束，所以数据采集完毕后，需要关闭采集器。但是不能调用stop方法去停止采集器，
      因为采集器需要源源不断的采集数据，默认情况下采集器不能关闭，只要调用stop方法，一个周期采集完后就结束程序了。
    4. 如果不关闭采集器，但是主程序已经执行完毕，采集器也躲不掉被关闭的命运。所以需要等到采集结束之后，再终止程序，其实就相当于不关。
      所以不能调用stop，也不能让主线程一直阻塞。这时就需要使用awaitTermination等待采集结束，去终止程序。
    5. 最终产生的效果就是，根据采集周期来获取数据，形成wordcount，源源不断的采集端口的数据，每3秒处理一次进行wordcount。
     */

    //启动采集器。 采集器只是一个守护线程，放在print后面可以。
    ssc.start()

    //默认情况下，采集器不能关闭
//    ssc.stop()


    //等到采集结束之后，终止程序
    ssc.awaitTermination()


  }

}
