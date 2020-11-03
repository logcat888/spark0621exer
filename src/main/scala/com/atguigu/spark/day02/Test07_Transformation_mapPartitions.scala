package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 9:32
 */
/*
转换算子：mapPartitions
 */
object Test07_Transformation_mapPartitions {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //以分区为单位，对RDD中的元素进行映射
    // 一般适用于批处理的操作，比如：将RDD中的元素插入到数据库中，需要数据库连接，
    // 如果每一个元素都创建一个连接，效率很低，可以对每个分区的元素，创建一个连接
//    val newRDD: RDD[Int] = rdd.mapPartitions(_.map(_ * 2))  //可读性低
//    rdd.mapPartitions(datas => datas.map(_ * 2)) 中datas.map(_ * 2)的map不叫算子，他只是集合中的方法，只有RDD中的方法叫算子，
    // 他只是在将一个分区的数据拿到后，当做一个集合（实实在在的集合）进行集合中map方法。
    val newRDD: RDD[Int] = rdd.mapPartitions(datas => datas.map(_ * 2))

    newRDD.collect().foreach(println)

    // 3.关闭资源
    sc.stop()
  }
}
