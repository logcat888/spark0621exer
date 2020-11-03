package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 21:38
 */
/*
转换算子--reduceByKey
--根据相同的key对RDD中的元素进行聚合
 */
object Test20_Transformation_reduceByKey {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 3), ("a", 5), ("b", 2)))

    val newRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)

    newRDD.collect().foreach(println)

    // 3.关闭资源
    sc.stop()
  }

}
