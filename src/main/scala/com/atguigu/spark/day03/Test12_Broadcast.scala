package com.atguigu.spark.day03

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-26 20:52
 */
/*
广播变量：分布式共享只读变量，只读是因为没有类似累加器的add方法，即便修改了变量中的值，
        对其他Executor也没有影响，只是会影响同一个Executor上的其他Task。
 */
object Test12_Broadcast {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象，该对象是Spark App的入口
    val sc = new SparkContext(conf)

    //想实现类似join效果  (("a",(1,4)),("b",(2,5)),("c",(3,6)))
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val list: List[(String, Int)] = List(("a",4),("b",5),("c",6))

    //创建一个广播变量
    val broadcastList: Broadcast[List[(String, Int)]] = sc.broadcast(list)

    val resRDD: RDD[(String, (Int, Int))] = rdd.map({
      case (k1, v1) => {
        var v3 = 0
        //没有创建广播变量时，每个Executor都会从Driver端拷贝一个List创建一个副本，当list比较大时，对Executor的内存消耗是非常大的。
//        for ((k2, v2) <- list; if k2 == k1) {
        //使用广播变量后对Executor端的内存压力会减少很多，只需要在每一个Executor上创建一个副本即可，不需要再每个一个Task上都创建副本
        // 一个Executor上可能会有多个Task
        for ((k2, v2) <- broadcastList.value; if k2 == k1) {
          //目前仅测试，不考虑，一个key，有多种value的情况
          v3 = v2
        }
        (k1, (v1, v3))
      }
    })
    resRDD.collect().foreach(println)



    // 3.关闭资源
    sc.stop()
  }

}
