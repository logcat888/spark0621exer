package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 9:55
 */
/*
mapPartitionsWithIndex：以分区为单位，对RDD中的元素进行映射，并且带分区编号
 */
object Test08_Transformation_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)

    //    val newRDD = rdd.map(_*2)
    //        val newRDD: RDD[Int] = rdd.mapPartitions(_.map(_ * 2))
    //解读index为分区号，datas为一组分区，对一组分区的数据map遍历，转变数据类型，添加上分区号,当然也可以不加。
    //    rdd.mapPartitionsWithIndex((index,datas) => datas.map((index,_)))
    // 什么处理都不做，原封不动输出
    rdd.mapPartitionsWithIndex((index,datas)=>datas)

    //需求：第二个分区数据*2，其余分区数据保持不变
//    val newRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, datas) => {
//      if (index == 1)
//        datas.map(data => (index, data * 2))
//      else
//        datas.map((index, _))
//    })

    val newRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, datas) => index match {
      case 1 => datas.map(data => (index,data * 2))
      case _ => datas.map(data => (index,data))
    })
    newRDD.collect().foreach(println)



    // 3.关闭资源
    sc.stop()
  }

}
