package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-24 19:03
 */
/*
转换算子--mapValues：对kv类型的RDD中的value部分进行映射
 */
object Test26_Transformation_mapValues {
  def main(args: Array[String]): Unit = {
      // 1.创建Spark配置文件对象
      val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

      // 2.创建SparkContext对象
      val sc = new SparkContext(conf)

      val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "d"), (2, "b"), (3, "c")))

      val newRDD: RDD[(Int, String)] = rdd.mapValues("|||" + _)

      newRDD.collect().foreach(println)

      // 3.关闭资源
      sc.stop()
    }

}
