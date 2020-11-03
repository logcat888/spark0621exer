package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 20:49
 */
/*
转换算子--partitionBy:对KV类型的RDD按照key进行重新分区
1. RDD本身是没有partitionBy这个算子，通过隐式转换动态给kv类型的RDD扩增的功能，在RDD的伴生对象中rddToPairRDDFunctions，
    partitionBy算子是PairRDDFunctions中的方法。
    implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
2. Spark提供的Partitioner(抽象类)有两种：HaskPartitioner（默认）以及RangePartitioner
3. 自定义分区器，需要继承Partitioner，实现抽象方法。
   1） getPartition：指定分区规则   返回值Int表示分区编号，从0开始
   注意：当返回的分区号大于partitions时，会抛异常。类似于MR中自定义分区个数与numReduceTask之间的关系
   2） numPartitions：获取分区个数
  }
 */
object Test19_Transformation_partitionBy {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //创建RDD
    //注意：RDD本身是没有partitionBy这个算子，通过隐式转换动态给kv类型的RDD扩增的功能
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1,"aaa"),(2->"bbb"),(3,"ccc")),3)
    println("==============分区前================")
    rdd.mapPartitionsWithIndex((index,datas)=> {
      println(index + "号分区" + datas.mkString(","))
      datas
    }).collect()
    /*
    2号分区(3,ccc)
    0号分区(1,aaa)
    1号分区(2,bbb)
     */
    println("==============分区后==============")
    val newRDD: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))
    newRDD.mapPartitionsWithIndex((index,datas)=> {
      println(index + "号分区" + datas.mkString(","))
      datas
    }).collect()
  /*
  1号分区(1,aaa),(3,ccc)
  0号分区(2,bbb)
   */

    // 3.关闭资源
    sc.stop()
  }

}

//自定义分区器
class MyPartitioner (partitions: Int) extends Partitioner{

  //获取分区个数
  override def numPartitions: Int = partitions

  //指定分区规则   返回值Int表示分区编号，从0开始
  //注意：当返回的分区号大于partitions时，会抛异常。类似于MR中自定义分区个数与numReduceTask之间的关系
  override def getPartition(key: Any): Int = {
    1
  }
}