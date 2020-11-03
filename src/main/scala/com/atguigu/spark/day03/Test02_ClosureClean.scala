package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-25 0:06
 */
/*
Kyro序列化
1.对性能要求很高，使用Kryo序列化也是一种优化方式，因为比Java轻。当数据比较多时，使用Kryo序列化效率会提升。
2.使用Kryo序列化，需要做的内容：1）替换默认的序列化机制；2）注册需要使用的Kryo序列化的自定义类，一个个的注册一下。

3.Spark存在序列化，但是并不是所有的序列化都使用Kryo序列化。java序列化是比较重的，会包含header，继承体系等。
	比如对于一个自定义的user(name,age)，java序列化之后是87字节，Kryo序列化之后是10字节。
	1）Kryo序列化效率这么高，但是Spark2.0只是部分序列化使用Kryo。是因为Kryo不能控制不参与序列化的属性,
	比如对name加上transient关键字，java序列化之后是44字节，而Kryo序列化之后还是10字节，与之前没有变化。
	2）目前使用的Spark版本，既有java序列化，也有Kryo序列化。也许在未来Spark像Hadoop一样实现自己的序列化，
	或者Kryo序列化更加完善。
	3）Spark使用Kryo的场景是，RDD在Shuffle数据时，简单数据类型，数组和字符串类型，这些Spark已经实现了。
	4）Serializable接口（特质）是一个标识性接口，说明他是可以序列化的，里面没有任何东西。
	trait Serializable extends scala.Any with java.io.Serializable
4. 在序列化时，在属性前加上transient关键字，用transient修饰的属性是不参与序列化的。
 */
object Test02_ClosureClean {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("SerDemo")
      .setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用kryo序列化的自定义类
      .registerKryoClasses(Array(classOf[Searcher]))

    //创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)

    //创建一个Searcher对象
    val searcher = new Searcher("hello")
    val result: RDD[String] = searcher.getMatchedRDD1(rdd)

    result.collect.foreach(println)
  }
}

case class Searcher(val query: String) {

  def isMatch(s: String) = {
    s.contains(query)
  }

  def getMatchedRDD1(rdd: RDD[String]) = {
    rdd.filter(isMatch)
  }

  def getMatchedRDD2(rdd: RDD[String]) = {
    val q = query
    rdd.filter(_.contains(q))
  }

}
