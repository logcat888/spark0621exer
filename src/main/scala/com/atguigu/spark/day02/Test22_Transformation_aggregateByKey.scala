package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 22:43
 */
/*
转换算子--aggregateByKey：按照key对分区内以及分区间的数据进行处理
1. 按照key对分区内和分区间的数据进行处理，适合于分区内与分区间之间不同业务逻辑的聚合，且可以赋初始值。
	1）场景举例：比如求出每个分区中同一组key的最大值，再对不同分区聚合求和。
	2）如果分区内和分区间的计算逻辑相同，就可以使用reduceBykey
2. 参数解读：函数柯里化实现闭包：aggregateByKey(初始值)(分区内计算规则，分区间计算规则)
 */
object Test22_Transformation_aggregateByKey {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    // 创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("c", 3), ("a", 5), ("b", 2),("b", 2), ("c", 3)),2)

    //reduceByKey实现wordcount
    rdd.reduceByKey(_+_).collect().foreach(println)
    /*
    (a,7)
    (b,4)
    (c,6)
     */

    println("===========================")
    //aggregateByKey实现wordcount
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
    /*
  (a,7)
  (b,4)
  (c,6)
   */

    println("=========================")
    //需求：分区最大值，求和.比如分区内最大值不能小于10，就可以给初始值为10，这里写0就行。
    //查看分区内的元素
    rdd.mapPartitionsWithIndex((index,datas) =>{
        println(index + "" + datas.mkString(","))
      datas
      }
    ).collect()
    /*
    0(a,2),(c,3),(a,5)
    1(b,2),(b,2),(c,3)
     */
    rdd.aggregateByKey(0)(Math.max(_,_),_+_).collect().foreach(println)
  /*
  (b,2)
  (a,5)
  (c,6)
   */

    // 3.关闭资源
    sc.stop()
  }

}
