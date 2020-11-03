package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 16:01
 */
/*
转换算子：coalesce/repartition：重新分区
1.Shuffle：将RDD中的数据打乱后重组叫Shuffle，没有Shuffle就是没有落盘。
2.对Shuffle的理解：Shuffle会落盘，可以将数据重新打乱组合，可以避免一些数据倾斜。不用Shuffle就不落盘，效率会好些。
  没有Shuffle好不好只说，具体看使用场景。比如缩减分区时不需要落盘，就不使用Shuffle。当然也可以Shuffle,防止数据倾斜。
3.关于coalesce与repartition的区别：
    1）coalesce：默认是不执行Shuffle，一般用于缩减分区。要是扩大分区，传入true即可。
    2）repartition:底层调用的就是coalesce，只不过默认是执行Shuffle，一般用于扩大分区
4. 源码：
    1）def coalesce(numPartitions: Int, shuffle: Boolean = false,
               partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
              (implicit ord: Ordering[T] = null)
      : RDD[T]
    2）def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    coalesce(numPartitions, shuffle = true)
  }

 */
object Test16_Transformation_coalesce {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)

    println("===========缩减分区之前===========")
    rdd.mapPartitionsWithIndex((index,datas)=>{
      println(index + "分区--->" + datas.mkString(","))
      datas
    }).collect()
    /*
    0分区--->1,2
    2分区--->5,6
    1分区--->3,4
     */
    println("===========缩减分区之后===========")
    val newRDD: RDD[Int] = rdd.coalesce(2)
    newRDD.mapPartitionsWithIndex((index,datas)=>{
      println(index + "分区--->" + datas.mkString(","))
      datas
    }).collect()
    /*
    0分区--->1,2
    1分区--->3,4,5,6
     */

    println("=============coalesce扩大分区===================")
    val newRDD1: RDD[Int] = rdd.coalesce(4)
    newRDD1.mapPartitionsWithIndex((index,datas)=>{
      println(index + "分区--->" + datas.mkString(","))
      datas
    }).collect()
    //注意：默认情况下，如果使用coalesce扩大分区是不起作用的，因为底层没有执行Shuffle，数据没有打乱重组，分区中的数据只是平移。
    /*
    1分区--->3,4
    2分区--->5,6
    0分区--->1,2
     */

    println("=============repartition扩大分区===================")
    //如果扩大分区，需使用热partition
    val newRDD2: RDD[Int] = rdd.repartition(4)
    newRDD2.mapPartitionsWithIndex((index,datas)=>{
      println(index + "分区--->" + datas.mkString(","))
      datas
    }).collect()
    /*
    0分区--->2,5
    1分区--->3,6
    3分区--->1
    2分区--->4
     */
    println("===========缩减分区之后Shuffle===========")
    val newRDD3: RDD[Int] = rdd.coalesce(2,true)
    newRDD3.mapPartitionsWithIndex((index,datas)=>{
      println(index + "分区--->" + datas.mkString(","))
      datas
    }).collect()
    /*
    0分区--->1,4,5
    1分区--->2,3,6
     */


    // 3.关闭资源
    sc.stop()
  }
}
