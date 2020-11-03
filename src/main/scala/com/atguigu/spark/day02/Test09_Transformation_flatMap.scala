package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 10:32
 */
/*
转换算子-flatMap:对集合中的元素进行扁平化处理
 val rdd1: RDD[Any] = sc.makeRDD(List(List(1, 2), List(3, 4), List(5, 6), List(7, 8), 9), 2)
    //注flatMap只能对元素为集合的进行扁平化
//    val newRDD1: RDD[Int] = rdd1.flatMap(datas => datas) //error
 */
object Test09_Transformation_flatMap {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1,2),List(3,4),List(5,6),List(7,8)),2)

    //注意：如果匿名函数输入和输出相同，那么不能简化
    val newRDD: RDD[Int] = rdd.flatMap(datas => datas)
//    rdd.flatMap(_) //error

    val rdd1: RDD[Any] = sc.makeRDD(List(List(1, 2), List(3, 4), List(5, 6), List(7, 8), 9), 2)
    //注flatMap只能对元素为集合的进行扁平化
//    val newRDD1: RDD[Int] = rdd1.flatMap(datas => datas) //error

    newRDD.collect().foreach(println)


    // 3.关闭资源
    sc.stop()
  }

}
