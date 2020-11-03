package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 22:43
 */
/*
转换算子--foldByKey：按照key对分区内和间的数据进行处理，有初始值
1. 是aggregateByKey的简化版，当分区内和分区间的业务逻辑相同时，可以简写成foldByKey，但是还是有初始值的，reduceByKey没有初始值。
2. 参数解读：函数柯里化实现闭包：foldByKey(初始值)(分区内和间计算规则)
3. 1）如果分区内和分区间计算规则一样，并且不需要指定初始值，那么可以优先使用reduceByKey
2）如果分区内和分区间计算规则一样，并且需要指定初始值，那么可以优先使用foldByKey
3）如果分区内和分区间计算规则不一样，并且需要指定初始值，那么可以优先使用aggregateByKey

 */
object Test23_Transformation_foldByKey {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    // 创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("c", 3), ("a", 5), ("b", 2),("b", 2), ("c", 3)),2)

    //reduceByKey实现wordcount
    //如果分区内和分区间计算规则一样，并且不需要指定初始值，那么可以使用reduceByKey
    rdd.reduceByKey(_+_).collect().foreach(println)
    /*
    (a,7)
    (b,4)
    (c,6)
     */

    println("===========================")
    //aggregateByKey实现wordcount
    //如果分区内和分区间计算规则不一样，并且需要指定初始值，那么可以优先使用aggregateByKey
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
    /*
  (a,7)
  (b,4)
  (c,6)
   */

    println("=========================")
    //foldByKey实现wordcount
    //如果分区内和分区间计算规则一样，并且需要指定初始值，那么可以优先使用foldByKey
    rdd.foldByKey(0)(_+_).collect().foreach(println)
    /*
    (b,4)
    (a,7)
    (c,6)
     */

    // 3.关闭资源
    sc.stop()
  }

}
