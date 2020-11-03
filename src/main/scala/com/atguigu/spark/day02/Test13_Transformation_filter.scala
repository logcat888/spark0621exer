package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 9:32
 */
/*
转换算子：filter
按照指定的过滤规则，对RDD中的元素进行过滤
 */
object Test13_Transformation_filter {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd= sc.makeRDD(List("xiaoming","xiaohong","bob","jack"), 2)

    val newRDD: RDD[String] = rdd.filter(_.contains("xiao"))



    newRDD.collect().foreach(println)

    // 3.关闭资源
    sc.stop()
  }
}
