package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 13:48
 */
/*
使用groupBy实现wordcount
 */
object Test12_WordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    /*
    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"), 2)

    //简单版——实现1
    //分割后扁平化
    val mapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    //元素类型转变为(word,1)
    val mapRDD1: RDD[(String, Int)] = mapRDD.map((_, 1))
    //按照key对RDD中的元素进行分组
    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupBy(_._1)
    //对分组后的元素再次进行映射,使用模式匹配
    val resRDD: RDD[(String, Iterable[(String, Int)])] = groupByRDD.map {
      case (word, datas) => (word, datas)
    }
    resRDD.collect().foreach(println)

    //简单版：实现2
    rdd.flatMap(_.split(" ")).groupBy(elem => elem).map(elem => (elem._1, elem._2.size)).collect().foreach(println)
    */

    //复杂版
    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hello scala", 2), ("Hello spark", 2), ("Hello world", 3)))

    rdd.flatMap(elem => {
      val word = elem._1.split(" ")
      val count = elem._2
      word.map((_, count))
    }).groupBy(_._1).map {
      case (word, datas) => (word, datas.map(_._2).sum)
    }.collect().foreach(println)

    // 3.关闭资源
    sc.stop()
  }
}
