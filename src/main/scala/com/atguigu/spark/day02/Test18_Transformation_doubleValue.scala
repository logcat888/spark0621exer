package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 19:35
 */
/*
转换算子--双value类型
1）并集
  union
2）交集
  intersect  --- intersection
3）差集
  diff --------- subtract
4）拉链
  zip要求：分区数必须一致，分区中元素的个数也必须一致
 */
object Test18_Transformation_doubleValue {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val rdd2: RDD[Int] = sc.makeRDD(List(4,5,6,7))

    //合集
    val newRDD1: RDD[Int] = rdd1.union(rdd2)
    newRDD1.collect().foreach(println) // 1 2 3 4 5 6 7

    //交集
    val newRDD2: RDD[Int] = rdd1.intersection(rdd2)
    newRDD2.collect().foreach(println) // 4

    //差集
    val newRDD3: RDD[Int] = rdd1.subtract(rdd2)
    newRDD3.collect().foreach(println) // 1 2 3

    //拉链
    // 要求：分区数必须一致，分区中元素的个数也必须一致
    //当rdd1与rdd2中的元素个数不匹配时，做拉链时会 报错  Can only zip RDDs with same number of elements in each partition
    //当两个RDD分区数不等时，做拉链也会报错  Can't zip RDDs with unequal numbers of partitions: List(2, 3)
    val newRDD4: RDD[(Int, Int)] = rdd1.zip(rdd2)
    newRDD4.collect().foreach(println) //(1,4) (2,5) (3,6) (4,7)

    // 3.关闭资源
    sc.stop()
  }

}
