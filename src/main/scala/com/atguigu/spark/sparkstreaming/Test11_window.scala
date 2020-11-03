package com.atguigu.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chenhuiup
 * @create 2020-10-02 15:32
 */
/*
有状态转换--window
1
 */
object Test11_window {
  def main(args: Array[String]): Unit = {

    //创建配置文件对象
    //注意：Streaming程序执行至少需要2个线程，一个用于接收器采集数据，一个用于处理数据，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("China").setMaster("local[*]")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）,指定 1)配置文件，以及 2)数据采集周期
    // 采集周期Duration，构造器默认单位是毫秒，可以通过伴生对象Seconds/Minutes方便指定采集周期
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    // 从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //设置窗口的大小以及滑动步长 ,这两个值都应该是采集周期的整数倍
    val windowDS: DStream[String] = socketDS.window(Seconds(3 * 2),Seconds(3))

    // 进行wordcount
    val resDS: DStream[(String, Int)] = windowDS.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //打印输出
//    resDS.print()
    // 输出数据到指定的文件
//    resDS.saveAsTextFiles("D:\\workspace_idea\\spark0621\\spark0621exer\\output\\RNG","zui")

    //foreachRDD的理解：
    // 1. 尽管一个采集周期只有一个DStream，底层是对一个RDD的封装，但是由于数据是源源不断过来的，所以
    // foreachRDD是相当于对源源不断过来的采集周期中的每一个RDD进行遍历，执行相同的处理逻辑。
    // 2. foreachRDD比较灵活，比如可以将RDD写入到数据库中
    // 3. 相当于行动算子
    resDS.foreachRDD(
      rdd => {
        // 将RDD中的数据保存到MySQL数据库中
        rdd.foreachPartition(
          // 连接mysql数据库，写入数据
          // 1)注册驱动
          // 2）获取连接
          // 3）创建数据库操作对象
          // 4）执行SQL语句
          // 5）处理结果集
          // 6）释放资源
          datas => {
            //.........
          }
        )
      }
    )

    //在DStream中使用累加器，广播变量,通过SparkContext对象设置
//    ssc.sparkContext.longAccumulator("uzi")
//    ssc.sparkContext.broadcast(10)

    //在DStream中使用缓存
//    resDS.cache()
//    resDS.persist(StorageLevel.MEMORY_ONLY)

    //在DStream中使用检查点
//    ssc.checkpoint("D:\\workspace_idea\\spark0621\\spark0621exer\\cp")

    //SparkSQl操作
    // 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导包
    import spark.implicits._
    //使用SparkSQL，立即执行，所以使用输出算子,每次输出的结果为一个采集周期内的数据
    resDS.foreachRDD(
      rdd => {
        //将RDD转换为DataFrame
        val df: DataFrame = rdd.toDF("word","count")
        //创建一个临时视图
        df.createOrReplaceTempView("t1")
        //执行SQL
        spark.sql("select * from t1").show()
      }
    )




    //启动采集器。 采集器只是一个守护线程，放在print后面可以。
    ssc.start()

    //等到采集结束之后，终止程序
    ssc.awaitTermination()
  }
}
