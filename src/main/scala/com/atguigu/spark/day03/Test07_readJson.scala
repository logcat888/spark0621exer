package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
 * @author chenhuiup
 * @create 2020-09-26 8:45
 */
/*
读取Json格式数据创建RDD
0. 一般数据传输的格式：
	1）json：本质还是文本文件，用[]表示多条json文件，用{}表示一条json数据，属性与值之间用:分割，多个属性之间用,分割。
		[{"name":"zhangsan","age":"18"},{"name":"lisi","age":"20"}]
	2）xml
1. Json本质还是文本文件，所以需要将json解析为对象，或将对象编译成json。一般转换时很少自己写json的转换框架，
    都是使用对json进行处理的第三方的框架，比如Json-lib，Jackson，fastJson，gJson。fastJson是阿里的开源
    技术，号称是最快的，但是有一些漏洞。
2. spark对Json格式文件进行解析处理时，需要将[] 、 逗号这些表示多个json的分割符号去掉，因为spark是一行一行的读取文本文件。
    尽管idea报错，认为接送格式不对，但是spark能够解析读取出来。
3. scala自带解析json格式的工具类：scala.util.parsing.json.JSON
 */
object Test07_readJson {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/test.json")

    //使用scala自带的json解析器对json文件进行解析
    val resRDD: RDD[Option[Any]] = rdd.map(JSON.parseFull)

    //情况一：没有对Json进行处理，删除[]，
//    resRDD.collect().foreach(println)
//    None
//    None
//    Some(Map(name -> lisi, age -> 20))
//    None

    //情况二：对json进行处理，删除[]，
    resRDD.collect().foreach(println)
    /*
    Some(Map(name -> zhangsan, age -> 18))
    Some(Map(name -> lisi, age -> 20))
     */


    // 3.关闭资源
    sc.stop()
  }
}
