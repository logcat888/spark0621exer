package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 16:01
 */
/*
转换算子：sample:随机抽样

抽样的应用场景：在做分析计算时，如果需要分组，可能会出现数据倾斜，可以使用sample对数据进行抽样，观察部分数据的特征，制定合适的分组规则

 */
object Test14_Transformation_sample {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

//    val rdd: RDD[Int] = sc.makeRDD(1 to 10)

    /*
     --withReplacement  是否抽样放回
     true  抽样放回
     false  抽样不放回
     -- fraction
     withReplacement=true   表示期望每一个元素出现的次数 >0
     withReplacement=false  表示RDD中每一个元素出现的概率(0,1)
     -- seed 抽样算法的初始值
     一般不需要指定
     */
    /*//从rdd中随机抽取一些数据(抽样放回)
    val newRDD: RDD[Int] = rdd.sample(true,1)

    //从rdd中随机抽取一些数据(抽样不放回)
    val newRDD: RDD[Int] = rdd.sample(false,0.6)

    newRDD.collect().foreach(println)*/

    //控制抽样个数
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))

    //第一个参数为不放回抽样，第二个参数控制抽样的个数，抽2个
    val res: Array[Int] = rdd.takeSample(false,2)  // 1 ，5

    println(res.mkString(","))

    // 3.关闭资源
    sc.stop()
  }
}
