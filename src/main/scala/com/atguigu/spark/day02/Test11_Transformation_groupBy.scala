package com.atguigu.spark.day02

import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 11:44
 */
/*
转换算子： groupBy：分组
1）分区与分组的区别：
    分区：是分配任务，每个分区在不同的Executor上执行。
    分组：按照一个的规则对RDD中的元素进行组合，组合后的数据可能会放到某一个分区里。
2）分区的概念大于分组，一个分区里可能有含有多个分组
3）让当前线程阻塞，以便在本地local:4040，看到程序的执行情况
4）只要groupBy就会经历Shuffle，Shuffle就是将RDD中的元素打乱重新组合分配，就涉及到落盘
5）groupBy()参数解读：K为RDD中的元素T经过一定规则后提取的key。 返回值RDD[(K, Iterable[T])] 中的元素为一个个二元组，
  二元组中的第二个元素为可迭代的集合，集合中的元素为同一个key中的元素。
def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    groupBy[K](f, defaultPartitioner(this))
  }
 */
object Test11_Transformation_groupBy {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),3)
    println("================groupBy分组前=================")
    rdd.mapPartitionsWithIndex((index,datas) =>{
      println(index + "--->" + datas.mkString(","))
      datas
    }).collect()

    /*
    1--->4,5,6
    2--->7,8,9
    0--->1,2,3
     */

    println("================groupBy分组后=================")
    rdd.groupBy(_ % 2).mapPartitionsWithIndex((index,datas) =>{
      println(index + "--->" + datas.mkString(","))
      datas
    }).collect()
    /*
       2--->
       1--->(1,CompactBuffer(1, 3, 5, 7, 9))
       0--->(0,CompactBuffer(2, 4, 6, 8))
     */

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),2)
    println("================groupBy分组前=================")
    rdd1.mapPartitionsWithIndex((index,datas) =>{
      println(index + "--->" + datas.mkString(","))
      datas
    }).collect()

    /*
    0--->1,2,3,4
    1--->5,6,7,8,9
     */

    println("================groupBy分组后=================")
    rdd1.groupBy(_ % 3).mapPartitionsWithIndex((index,datas) =>{
      println(index + "--->" + datas.mkString(","))
      datas
    }).collect()
    /*
    1--->(1,CompactBuffer(1, 4, 7))
    0--->(0,CompactBuffer(3, 6, 9)),(2,CompactBuffer(2, 5, 8))
     */

    //让当前线程阻塞，以便在本地local:4040，看到程序的执行情况
    TimeUnit.MINUTES.sleep(3)

    // 3.关闭资源
    sc.stop()
  }

}
