package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-23 18:34
 */
/*
转换算子--sortBy
对RDD中的元素进行排序，默认按照字典序
注：
1. sortBy，默认不改变分区，底层使用sortByKey()
2. sortBy, 需要经过Shuffle，底层创建ShuffledRDD对象
3. 判断某个算子是否经过Shuffle的两种方法：
    1） 让程序sleep一会，看localhost:4040，是否出现Shuffle;
    2)  看底层源码是否创建ShuffledRDD对象
  def sortBy[K](
      f: (T) => K,
      ascending: Boolean = true,
      numPartitions: Int = this.partitions.length)
      (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = withScope {
    this.keyBy[K](f)
        .sortByKey(ascending, numPartitions)
        .values
  }

    def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      : RDD[(K, V)] = self.withScope
  {
    val part = new RangePartitioner(numPartitions, self, ascending)
    new ShuffledRDD[K, V, V](self, part)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }
 */
object Test17_Transformation_sortBy {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 4, 3, 2))

    //升序排序
    val newRDD: RDD[Int] = rdd.sortBy(num => num)

    //降序排序
    val newRDD1: RDD[Int] = rdd.sortBy(num => -num)
    val newRDD2: RDD[Int] = rdd.sortBy(num => num,false)

    newRDD.collect().foreach(println) // 1 2 3 4
    newRDD.foreach(println) //3 4 2 1


    val rdd1: RDD[String] = sc.makeRDD(List("1","4","3","22"))
    //按照字符串字符字典顺序进行排序
    val newRDD3: RDD[String] = rdd1.sortBy(elem => elem)
    newRDD3.collect().foreach(println) //1  22  3  4

    //需求：按照字符串转换为整数后的大小进行排序
//    val newRDD4: RDD[String] = rdd1.sortBy(elem => elem.toInt)
    //匿名函数输入和输出一样，不能省略，如果不一样，只出现一次，可以省略。
    val newRDD4: RDD[String] = rdd1.sortBy(_.toInt)
    newRDD4.collect().foreach(println)  // 1  3  4  22
    // 3.关闭资源
    sc.stop()
  }

}
