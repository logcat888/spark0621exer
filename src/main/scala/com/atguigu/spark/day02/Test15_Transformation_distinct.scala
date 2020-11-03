package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 16:01
 */
/*
转换算子：distinct
0. 分区的目的就是提高并发度，每个分区都在一个Executor上执行。
1. distinct有2个重载的方法：一个无参,不改变分区数；一个有参数（最小分区数numPartitions），
  比如当我们有100W条数据，有10个分区，去重之后假设还剩10W条数据，那么就没有必要开10个分区，
  这时可以调小分区数。分区的目的就是提高并发度。
  1）  def distinct(): RDD[T] = withScope {distinct(partitions.length)}
  2）  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    coalesce(numPartitions, shuffle = true)
  }
2. 将分区的数据打乱重组叫Shuffle，发现distinct后分区的数据发生变化，说明distinct会发生Shuffle。
3. 不管指定不指定distinct的参数都会发生Shuffle。
4. distinct核心源码：1)map前省略了this. 就是当前RDD;2)(x, _) => x  其实就是（null,null）=> null；3）map(_._1)只取元组的第一个元素，
  也就是只有key，不要null。
   map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)

 */
object Test15_Transformation_distinct {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,1,1,1,2,2,2,5,5,5,3,3,3),5)
    println("===========没有去重之前===========")
    rdd.mapPartitionsWithIndex((index,datas)=>{
      println(index + "分区--->" + datas.mkString(","))
      datas
    }).collect()
/*
4分区--->3,3,3
3分区--->5,5,5
2分区--->2,2
1分区--->1,1,2
0分区--->1,1
 */
    println("===========去重之后===========")
    val newRDD:RDD[Int] = rdd.distinct()
    newRDD.mapPartitionsWithIndex((index,datas)=>{
      println(index + "分区--->" + datas.mkString(","))
      datas
    }).collect()
/*
4分区--->
3分区--->3
0分区--->5
2分区--->2
1分区--->1
 */
    println("=============调小分区==============")
    val newRDD1:RDD[Int] = rdd.distinct(2)
    newRDD1.mapPartitionsWithIndex((index,datas)=>{
      println(index + "分区--->" + datas.mkString(","))
      datas
    }).collect()
    /*
 0分区--->2
1分区--->1,3,5
     */
    // 3.关闭资源
    sc.stop()
  }
}
