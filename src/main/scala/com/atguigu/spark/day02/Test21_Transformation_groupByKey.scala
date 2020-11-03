package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 21:58
 */
/*
转换算子--groupByKey
1) 根据key对RDD中的元素进行分组,将所有的value放入一个seq集合中，并不进行聚合。经过Shuffle。
2）对于groupBy是对RDD的元素，按照key进行分组，将元素放入到集合中，这个元素可以是任意类型。
3）groupByKey有三个重载的方法：
  3.1）空参：底层调用默认的HashPartitioner进行分组
    def groupByKey(): RDD[(K, Iterable[V])] = self.withScope {
    groupByKey(defaultPartitioner(self))
  }
  3.2）有参数：指定分区器
  def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]
  3.3）有参数：指定分区数
  def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = self.withScope {
    groupByKey(new HashPartitioner(numPartitions))
  }
 */
object Test21_Transformation_groupByKey {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 3), ("a", 5), ("b", 2)))

    //根据key对RDD进行分组
    val newRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    newRDD.collect().foreach(println)
    /*
    (a,CompactBuffer(2, 5))
    (b,CompactBuffer(3, 2))
     */

    val newRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    newRDD1.collect().foreach(println)
    /*
    (a,CompactBuffer((a,2), (a,5)))
    (b,CompactBuffer((b,3), (b,2)))
     */

    //需求，对单词出现的个数求和
    //注：因为二元组是一个整体，所以必须写成一个元素，再对其进行操作；或者使用模式匹配，匹配元组，这样就可以使用二元组中的变量了。
    // 如果写成下面的错误操作的写法，那么编译器认为是两个参数，编译不通过。
    // 其实下面的写法就是偏函数的写法，所以case外的花括号不能写成圆括号，因为外面省略了  变量 match。
//    newRDD.map((word,count) =>(word,count.sum)) //error
    newRDD.map(elem => (elem._1,elem._2.sum))
    val res: RDD[(String, Int)] = newRDD.map({
      case (word, count) => (word, count.sum)
    })
    res.collect().foreach(println)

    // 3.关闭资源
    sc.stop()
  }

}
