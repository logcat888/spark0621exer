package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-22 23:48
 */
object Test06_Transformation_map {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    println("原分区个数：" + rdd.partitions.length)

    val newRDD: RDD[Int] = rdd.map(_ * 2)
    println("原分区个数：" + newRDD.partitions.length)
    newRDD.partitions.foreach(println)
    println("===================")
//    org.apache.spark.rdd.ParallelCollectionPartition@691
//    org.apache.spark.rdd.ParallelCollectionPartition@692

    newRDD.collect().foreach(println)
    // 3.关闭资源
    sc.stop()
  }

}
