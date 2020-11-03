package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-22 18:47
 */
/*
默认分区：
1）从集合中创建RDD
  取决于分配给应用的CPU的核数

2）读取外部文件创建RDD
  math.min(取决于分配给应用的CPU核数，2)
 */
object Test03_Partiton_default {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //通过集合创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //查看分区效果
    println(rdd.partitions.size) //查看分区个数 12 ，为CPU核心数
//    print(rdd.saveAsTextFile("output"))

    //通过外部文件创建RDD
    val rdd1: RDD[String] = sc.textFile("input")
    //查看分区效果
    println(rdd1.partitions.size) //2
    println(rdd1.getNumPartitions) //2

    // 3.关闭资源
    sc.stop()
  }
}
