package com.atguigu.spark.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-29 15:17
 */
/*
求平均年龄 --- 累加器方式实现
 */
object Test04_UDAF_Accumulator {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //创建一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("xiaohong", 18), ("zhangsan", 25), ("xiaoming", 30)))

    //方案二：使用累加器

    //创建累加器，注册累加器
    val acc: AvgAccumulator = new AvgAccumulator
    sc.register(acc)

    //使用累加器
    rdd.foreach(acc.add)

    //获取累加器的值
    val res: (Int, Int) = acc.value
    println(res)

    println(res._1.toDouble / res._2)

    // 3.关闭资源
    sc.stop()
  }

}

//创建一个累加器
class AvgAccumulator extends AccumulatorV2[(String, Int), (Int, Int)] {
  //定义累加变量
  var (age, count): (Int, Int) = Tuple2[Int, Int](0, 0)
  //判断累加器变量是否为初始状态，以便通过闭包检测
  override def isZero: Boolean = age == 0 && count == 0
  //从Driver端拷贝累加器
  override def copy(): AccumulatorV2[(String, Int), (Int, Int)] = new AvgAccumulator
  //将Driver端拷贝的累加器置空，以便通过闭包检测
  override def reset(): Unit = Tuple2[Int, Int](0, 0)
  //向累加变量中添加元素
  override def add(v: (String, Int)): Unit = {
    age += v._2
    count += 1
  }
  //Driver端合并两个累加器的累加变量
  override def merge(other: AccumulatorV2[(String, Int), (Int, Int)]): Unit = {
    age += other.value._1
    count += other.value._2
  }
  //获取累加器的累加变量
  override def value: (Int, Int) = (age, count)
}
