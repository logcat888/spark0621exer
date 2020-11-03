package com.atguigu.spark.day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author chenhuiup
 * @create 2020-09-22 20:11
 */
/*
从集合中创建RDD指定分区数：
1）默认分区规则
  取决于分配给应用的CPU的核数

2）指定分区数
  math.min(取决于分配给应用的CPU核数，2)
 */
object Test04_Partition_mem {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //通过集合创建RDD
//    //集合中4个数据--默认分区数--实际输出12个分区--分区中数据分布 2分区->1,  5分区->2,  8分区->3,  11分区->4
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))


//    //集合中4个数据--指定分区数为4--实际输出4个分区--分区中数据分布 0分区->1,  1分区->2,  2分区->3,  3分区->4
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),4)

//    //集合中4个数据--指定分区数为3--实际输出3个分区--分区中数据分布 0分区->1,  1分区->2,  2分区->3、4
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),3)

    //集合中5个数据--指定分区数为3--实际输出3个分区--分区中数据分布 0分区->1,  1分区->2,  2分区->3、4。计算索引
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5),3)


    //查看分区效果
    println(rdd.partitions.size) //查看分区个数 12 ，为CPU核心数
    print(rdd.saveAsTextFile("output"))


    // 3.关闭资源
    sc.stop()
  }
}
