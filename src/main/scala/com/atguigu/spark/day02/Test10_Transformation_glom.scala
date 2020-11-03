package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 10:49
 */
/*
转换算子--glom：将RDD一个分区中的元素，组合胃一个新的数组
 */
object Test10_Transformation_glom {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    println("==========没有glom之前===============")
    // datas为一组分区的数据，数据为一个个元素
    rdd.mapPartitionsWithIndex((index, datas) => {
      println(index + "号分区--" + datas.mkString(","))
      datas
    }).collect()

    println("==========调用glom之后===============")
    rdd.glom().mapPartitionsWithIndex((index, datas) => {
      println(index + "号分区--" + datas.mkString(","))
      datas
    }).collect()
    // 0号分区--[I@79a6397b  1号分区--[I@25147860

    //datas为一组分区的数据，数据为一个数组，相当于集合中装的是一个个数组，需要将集合中的元素取出来再对数组mkString，
    // 否则mkString的是一个个数组，且该分区只有一个元素，该元素为一个数组
    rdd.glom().mapPartitionsWithIndex((index, datas) => {
      println(index + "号分区--" + datas.next().mkString(","))
      datas
    }).collect()
    //1号分区--4,5,6   0号分区--1,2,3

    println("==================")
    //需求：求每个分区的最大值
    rdd.glom().mapPartitionsWithIndex((index, datas) => datas.map(arr => (index, arr.max))).collect().foreach(println)
    //  (0,3)  (1,6)
    // 需求：求每个分区的最大值，并求和 //(0,9)
    println(rdd.glom().mapPartitionsWithIndex((index, datas) => datas.map(arr => (index, arr.max))).reduce((v1, v2) => (0, v1._2 + v2._2)))
    println(rdd.glom().map(_.max).collect().sum)
    // 3.关闭资源
    sc.stop()
  }
}
