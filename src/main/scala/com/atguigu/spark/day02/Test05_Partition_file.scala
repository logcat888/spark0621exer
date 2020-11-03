package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-22 22:32
 */
/*
通过读取外部文件，创建RDD
-- 默认分区规则
    math.min(分配给应用的CPU核数，2)
-- 指定分区
    >1. 在textFile方法中，第二个参数minPartitions，表示最小分区数
        注意：是最小，不是实际的分区个数
     2. 在实际计算分区个数的时候，会根据文件的总大小和最小分区数进行相除运算
        1）如果余数为0
              那么最小分区数，就是最终实际的分区数
        2）如果余数不为0
              那么实际的分区数，要计算
 */
object Test05_Partition_file {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //从本地文件中读取数据，创建RDD

    //输入数据 1换行2换行3换行4 ，采用默认分区方式，最终分区数为 2 ， 0号分区1， 2 ，1号分区3， 4
//    val rdd: RDD[String] = sc.textFile("input/2.txt")


//    //输入数据 1换行2换行3换行4 （10字节），minPartitions设置为3，最终分区数为 4 ， 0号分区1， 2  ；1号分区 3   ； 2号分区 4  ； 3号分区 空
//    val rdd: RDD[String] = sc.textFile("input/2.txt",3)
//
//    //输入数据 123456(单行无空格) （6字节） ，minPartitions设置为3，最终分区数为 3 ， 0号分区123456 ；1号分区  空 ； 2号分区空
//    val rdd: RDD[String] = sc.textFile("input/2.txt",3)

    //输入数据 123换行4567 （9字节），minPartitions设置为3，最终分区数为 3 ， 0号分区123 ；1号分区4567 ； 2号分区空
    val rdd: RDD[String] = sc.textFile("input/2.txt",3)




    rdd.saveAsTextFile("output")



    // 3.关闭资源
    sc.stop()
  }

}
