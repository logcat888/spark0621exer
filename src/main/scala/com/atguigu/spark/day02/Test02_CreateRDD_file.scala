package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-22 18:36
 */
object Test02_CreateRDD_file {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //从本地文件中读取数据，创建RDD
    val rdd: RDD[String] = sc.textFile("input")

    //从HDFS上读取数据，创建RDD
//    val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:8020/input")

    rdd.collect.foreach(println)

    // 3.关闭资源
    sc.stop()


  }

}
