package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 通过读取内存集合中的数据，创建RDD
 *
 * @author chenhuiup
 * @create 2020-09-22 18:00
 */
object Test01_CreateRDD_mem {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4)
    // 根据集合创建RDD的方式一
    //    val rdd:RDD[Int] = sc.parallelize(list)

    //根据集合创建RDD的方式二 : 底层调用的是parallelize(seq, numSlices)
    val rdd: RDD[Int] = sc.makeRDD(list)
    rdd.collect().foreach(println)

    // 3.关闭资源
    sc.stop()


  }

}
