package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author chenhuiup
 * @create 2020-09-26 15:52
 */
/*
自定义累加器：统计出RDD中，所有以“H”开头的单词以及出现次数（word,count）
1. 学习LongAccumulator累加器如何使用
  def longAccumulator: LongAccumulator = {
    val acc = new LongAccumulator //创建累加器
    register(acc) //注册累加器
    acc
  }
2. 累加器的好处就是减少了Shuffle的过程，效率会更高一些
3. 自定义累加器的步骤：
  1）继承AccumulatorV2，明确输入和输出泛型
  2）重写6个抽象方法
  3）创建累加器对象
  4）Driver端用SparkContext对象注册（register）累加器
  5）在Executor端使用累加器
 */
object Test11_MyAccumulator {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Haha", "Haha","HeHe", "Spark"))

    //创建累加器对象
    val myAcc = new MyAccumulator

    //注册累加器
    sc.register(myAcc)

    //使用累加器
    rdd.foreach(data => {
      myAcc.add(data)
    })

    //输出累加器结果
    println(myAcc.value)

    // 3.关闭资源
    sc.stop()
  }

}

//定义一个类，继承AccumulatorV2
//泛型累加器输入和输出数据的类型
class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {

  //定义一个集合，记录单词以及出现次数
  //  private val map: Seq[(String, Int)] => mutable.Map[String, Int]
  //   = mutable.Map[String,Int]
  var map: mutable.Map[String, Int] = mutable.Map[String, Int]() //创建Map集合必须加上()，否则只是类型的声明

  //是否为初始状态
  override def isZero: Boolean = map.isEmpty

  //拷贝
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val newMyAcc = new MyAccumulator
    newMyAcc.map = this.map
    newMyAcc
    //方案二：返回一个新的累加器供Executor端使用，Executor端使用的累加器必须是空的累加器，才能通过闭包检查
    // new MyAccumulator
  }


  //重置
  override def reset(): Unit = map.clear()

  //向累加器中添加元素
  override def add(v: String): Unit = {
    if (v.contains("H")) {
      //向可变集合中添加或更新元素
      map(v) = map.getOrElse(v, 0) + 1
    }
  }

  //合并,将两个分区的map集合合并
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    //    foldLeft[B](z : B)(op : scala.Function2[B, A, B]) : B  //初始值可以和输入类型不一致
    // fold（初始值:A）（运算规则 （A,A）=>A） 初始值类型，与输入参数类型一致，不能操作

        //当前Executor的map
        var map1 = map
        //另一个Executor的map
        var map2 = other.value

        map = map1.foldLeft(map2)(
          (mergeMap,kv) =>{
            var word = kv._1
            var count = kv._2
            mergeMap(word) = mergeMap.getOrElse(word,0) + count
            mergeMap
          }
        )

    /*  方法二：两个map合并
    other.value.foreach{
      case (word,count) => {
        map(word) = map.getOrElse(word,0) + count
      }
    }
    */
  }

  //获取累加器的值
  override def value: mutable.Map[String, Int] = map
}

