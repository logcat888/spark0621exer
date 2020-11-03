package com.atguigu.spark.day01

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-21 21:10
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    //2. 获取配置文件
    val conf = new SparkConf()
    // 2.1设置Master运行模式，设置application名字
    conf.setMaster("local[2]").setAppName("uzi")

    //1.获取SparkContext对象
    val sc = new SparkContext(conf)

    // 3.sc获取文件，执行RDD操作，最后collect()执行
//    val result: Array[(String, Int)] = sc.textFile("D:\\workspace_idea\\spark0621\\spark0621exer\\input")
//      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
//    result.foreach(println)

    // 3.为了能够打包在集群运行，输入路径和输出路径不能写死，通过main()传入参数
    // textFile（）文件输入路径  saveAsTextFile()文件输出路径
    sc.textFile(args(0))
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile(args(1))

    //4.释放资源
    sc.stop()
  }

}
