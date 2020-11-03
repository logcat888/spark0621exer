package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-24 19:38
 */
/*
转换算子：
1.join ：类似于内连接
2.leftOuterJoin : 类似于 左外连接 会将连接对象包装成Option，防止空指针
3.rightOuterJoin : 类似于 右外连接 会将连接对象包装成Option，防止空指针
4.fullOuterJoin ： 类似于 全外连接 会将连接对象包装成Option，防止空指针

转换算子：cogroup
1. 先将一个RDD中的按照key将value聚合成一个集合，再与另一个集合聚合，会保留有一个没有的，全都要

 */
object Test27_Transformation_join {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))

    //3.2 创建第二个pairRDD
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6), (2,8)))

    //join算子相当于内连接，将两个RDD中的key相同的数据匹配，如果key匹配不上，那么数据不关联。
    //注意：rdd 与 rdd1不一样
    val newRDD: RDD[(Int, (String, Int))] = rdd.join(rdd1)
    newRDD.collect().foreach(println)
    /*
    (1,(a,4))
    (2,(b,5))
    (2,(b,8))
     */
    val newRDD1: RDD[(Int, (Int, String))] = rdd1.join(rdd)
    newRDD1.collect().foreach(println)
    /*
    (1,(4,a))
    (2,(5,b))
    (2,(8,b))
     */

    println("===========左连接==============")
    rdd.leftOuterJoin(rdd1).collect().foreach(println)
    /*
    (1,(a,Some(4)))
    (2,(b,Some(5)))
    (2,(b,Some(8)))
    (3,(c,None))
     */
    println("===========右连接==============")
    val newRDD2: RDD[(Int, (Option[String], Int))] = rdd.rightOuterJoin(rdd1)
    newRDD2.collect().foreach(println)
    /*
    (1,(Some(a),4))
    (2,(Some(b),5))
    (2,(Some(b),8))
    (4,(None,6))
     */

    println("============全连接=============")
    rdd.fullOuterJoin(rdd1).collect().foreach(println)
    /*
    (1,(Some(a),Some(4)))
    (2,(Some(b),Some(5)))
    (2,(Some(b),Some(8)))
    (3,(Some(c),None))
    (4,(None,Some(6)))
     */

    //cogroup
    val newRDD3: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd.cogroup(rdd1)
    newRDD3.collect().foreach(println)
    /*
    (1,(CompactBuffer(a),CompactBuffer(4)))
    (2,(CompactBuffer(b),CompactBuffer(5, 8)))
    (3,(CompactBuffer(c),CompactBuffer()))
    (4,(CompactBuffer(),CompactBuffer(6)))
     */

    // 3.关闭资源
    sc.stop()
  }

}
