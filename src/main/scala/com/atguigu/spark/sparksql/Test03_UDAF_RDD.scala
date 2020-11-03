package com.atguigu.spark.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-29 7:50
 */
/*
求平均年龄 --- RDD算子方式实现
 */
object Test03_UDAF_RDD {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //创建一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("xiaohong",18),("zhangsan",25),("xiaoming",30)))

    //方案一：使用reduceByKey求平均数
    val resRDD: RDD[Double] = rdd.map(data => (1, (data._2, 1)))
      .reduceByKey((u1, u2) => (u1._1 + u2._1, u2._2 + u1._2)).map({
      case (_, (age, count)) => age.toDouble / count
    })
    resRDD.collect().foreach(println) // 24.333333333333332

    //方案二：使用累加器


    //方案三：使用combineByKey

    //方案四：使用reduce
    val res: (Int, Int) = rdd.map({
      case (name, age) => (age, 1)
    }).reduce({
      case ((age1, count1), (age2, count2)) => (age1 + age2, count1 + count2)
    })
    println(res._1.toDouble / res._2) //24.333333333333332

    // 3.关闭资源
    sc.stop()
  }
}
