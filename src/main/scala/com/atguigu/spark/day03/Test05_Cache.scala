package com.atguigu.spark.day03

import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-25 22:21
 */
/*
RDD的缓存
 */
object Test05_Cache {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello scala","hello spark"),2)

    //扁平映射
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    //结构转换
    val newRDD: RDD[(String, Long)] = flatMapRDD.map(data =>{
      println("******************")
      (data, System.currentTimeMillis())
    })
    //对RDD数据进行缓存，底层调用的persist函数  默认缓存在内存中
    //注意：虽然叫持久化，但是当应用程序执行结束之后，缓存的目录也会被删除。即便是缓存在磁盘上，也是会被删除。
//    newRDD.cache()

    //persist可以接收参数，指定缓存位置，尽管可以往磁盘上缓存，但是这种用法不多
    newRDD.persist(StorageLevel.DISK_ONLY)

    //打印血缘关系
    println(newRDD.toDebugString)
//    (2) MapPartitionsRDD[2] at map at Test05_Cache.scala:30 [Memory Deserialized 1x Replicated]
//    |  MapPartitionsRDD[1] at flatMap at Test05_Cache.scala:27 [Memory Deserialized 1x Replicated]
//    |  ParallelCollectionRDD[0] at makeRDD at Test05_Cache.scala:24 [Memory Deserialized 1x Replicated]

    newRDD.collect().foreach(println)
    /*
    ****************
    * **************
    * **************
    * **************
    (hello,1601044587548)
    (scala,1601044587549)
    (hello,1601044587548)
    (spark,1601044587548)
     */
    println("========================")

    //打印血缘关系
    println(newRDD.toDebugString)
//    (2) MapPartitionsRDD[2] at map at Test05_Cache.scala:30 [Memory Deserialized 1x Replicated]
//    |       CachedPartitions: 2; MemorySize: 464.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B
//      |  MapPartitionsRDD[1] at flatMap at Test05_Cache.scala:27 [Memory Deserialized 1x Replicated]
//    |  ParallelCollectionRDD[0] at makeRDD at Test05_Cache.scala:24 [Memory Deserialized 1x Replicated]

    newRDD.collect().foreach(println)
    /*
   (hello,1601044587548)
  (scala,1601044587549)
  (hello,1601044587548)
  (spark,1601044587548)
     */

    TimeUnit.MINUTES.sleep(5)

    // 3.关闭资源
    sc.stop()
  }

}
