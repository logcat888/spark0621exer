package com.atguigu.spark.day03

import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-25 22:21
 */
/*
RDD的缓存
 */
object Test06_Checkpoint {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)
    //设置检查点目录
    sc.setCheckpointDir("./output")

//    //开发环境，应该将检查点目录设置在hdfs上
//    // 设置访问HDFS集群的用户名
//    System.setProperties("System.setProperty("HADOOP_USER_NAME","atguigu")
//      // 需要设置路径.需要提前在HDFS集群上创建/checkpoint路径
//      sc.setCheckpointDir("hdfs://hadoop102:9000/checkpoint")


    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello scala","hello spark"),2)

    //扁平映射
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    //结构转换
    val newRDD: RDD[(String, Long)] = flatMapRDD.map(data =>{
      (data, System.currentTimeMillis())
    })

    println("============1============")
    //打印血缘关系
    println(newRDD.toDebugString)
//    (2) MapPartitionsRDD[2] at map at Test06_Checkpoint.scala:31 []
//    |  MapPartitionsRDD[1] at flatMap at Test06_Checkpoint.scala:28 []
//    |  ParallelCollectionRDD[0] at makeRDD at Test06_Checkpoint.scala:25 []

    //在开发环境，一般检查点和缓存配合使用
    newRDD.cache()

    //设置检查点
    newRDD.checkpoint()

    newRDD.collect().foreach(println)
    /*
    (hello,1601048172595)
    (scala,1601048172595)
    (hello,1601048172595)
    (spark,1601048172595)
     */
    println("===========2=============")

    //打印血缘关系
    println(newRDD.toDebugString)
//    (2) MapPartitionsRDD[2] at map at Test06_Checkpoint.scala:31 []
//    |  ReliableCheckpointRDD[3] at collect at Test06_Checkpoint.scala:41 []
    newRDD.collect().foreach(println)
    /*
    (hello,1601048172705)
    (scala,1601048172705)
    (hello,1601048172705)
    (spark,1601048172705)
     */

    //释放缓存
    newRDD.unpersist()
    println("===========3=============")
    println(newRDD.toDebugString)
//    (2) MapPartitionsRDD[2] at map at Test06_Checkpoint.scala:31 []
//    |  ReliableCheckpointRDD[3] at collect at Test06_Checkpoint.scala:41 []
    newRDD.collect().foreach(println)
    /*
    (hello,1601048172705)
    (scala,1601048172705)
    (hello,1601048172705)
    (spark,1601048172705)
     */
    TimeUnit.MINUTES.sleep(5)

    // 3.关闭资源
    sc.stop()
  }

}
