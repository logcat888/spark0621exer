package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 22:43
 */
/*
转换算子--combineByKey：

 */
object Test24_Transformation_combineByKey {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    // 创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Bob", 89), ("Ailce", 92), ("Bob", 75), ("Ailce", 82), ("Bob", 62), ("Ailce", 73)), 2)
    //需求：求所有学生的平均成绩
    rdd.map(elem => (1, elem._2)).groupByKey().map(elem => elem._2.sum / elem._2.size).collect().foreach(println) //78

    //需求：求每个学生的平均成绩
    //方案一：使用groupByKey
    //不是最优的方案，有可能某个key对应的value值特别大，同一组的数据肯定会在同一个分区，会发生数据倾斜，造成单点压力。
    // 比如统计每个地区的订单情况，北京的肯定比西藏的订单多
    //如果分组之后某个组数据量比较大，会造成单点压力
    rdd.groupByKey().map(elem => (elem._1, elem._2.sum / elem._2.size)).collect().foreach(println)
    /*
    (Ailce,82)
    (Bob,75)
     */

    //方案二：使用reduceByKey求平均值
    //效率比groupByKey效率高些
    rdd.map({
      case (name, score) => (name, (score, 1))
    }).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).map({
      case (name, score) => (name, score._1 / score._2)
    }).collect().foreach(println)

    println("==================================")
    //方案三： 通过combineByKey算子，
    //效率和reduceByKey一样，只是对初始值做了类型转换。
    /*
    createCombiner: V => C,  对RDD中当前key取出的第一个value做一个初始化，可以转化类型
    mergeValue: (C, V) => C,  分区内计算规则，主要在分区内进行，将当前分区的value值，合并到初始化得到的C上面
    mergeCombiners: (C, C) => C, 分区间计算规则
     */
    // 注：由于初始值转换数据类型为元组后，编译器无法自动识别，所以之后使用到元组后需要声明元组的类型。
    // 因为底层调用时没有加泛型，所以不能类型推断。combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)
    rdd.combineByKey(
      //将每个key的第一个value，转换成元组类型，方便计数
      zeroElem => (zeroElem, 1),
      // 注：由于初始值转换数据类型为元组后，编译器无法自动识别，所以之后使用到元组后必须要声明元组的类型。
      (elem: (Int, Int), value) => (elem._1 + value, elem._2 + 1),
      (elem1: (Int, Int), elem2: (Int, Int)) => (elem1._1 + elem2._1, elem1._2 + elem2._2)
    ).map({
      case (name,(score,count)) => (name,score / count)
    }).collect().foreach(println)

    println("========================")
    //如果元组的类型可以推断出来，可以省略
    val newRDD:RDD[(String,(Int,Int))] = rdd.combineByKey(
      //第一个参数，转换类型
      (_,1),
      //第二个参数，分区内计算逻辑，注意类型不一致
      (elem1,elem2) => (elem1._1 + elem2,elem1._2 + 1),
      //第三个参数，分区间计算逻辑
      (elem1,elem2) => (elem1._1 + elem2._1,elem1._2 + elem2._2)
    )
    newRDD.mapValues({
      case (score,count) => score/count
    }).collect().foreach(println)

    // 3.关闭资源
    sc.stop()
  }

}
