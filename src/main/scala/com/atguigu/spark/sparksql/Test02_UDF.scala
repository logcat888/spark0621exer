package com.atguigu.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author chenhuiup
 * @create 2020-09-29 7:20
 */
/*
自定义UDF函数(输入一行返回一行)，在每一个查询的名字前，加问候语
自定义UDF步骤：
1）使用SparkSession对象调用udf算子，register函数
2）register参数解读：第一个参数是函数名，第二个参数匿名函数，由于无法类型推断，必须把参数的类型加上
 */
object Test02_UDF {
  def main(args: Array[String]): Unit = {
    // 创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("hello")
    // 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 创建DF
    val df: DataFrame = spark.read.json("D:\\workspace_idea\\spark0621\\spark0621exer\\input\\test.json")

    // 注册自定义函数
    //register参数解读：第一个参数是函数名，第二个参数匿名函数，由于无法类型推断，必须把参数的类型加上
    val addSayHi: UserDefinedFunction = spark.udf.register("addSayHi", (username:String) => {"nihao:" + username})

    // 创建临时视图
    df.createOrReplaceTempView("user")

    // 通过SQL语句，从临时视图查询数据
    spark.sql("select addSayHi(name),age from user").show()

    // 关闭资源
    spark.stop()

  }
}
