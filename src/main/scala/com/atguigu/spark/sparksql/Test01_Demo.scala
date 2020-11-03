package com.atguigu.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author chenhuiup
 * @create 2020-09-28 22:39
 */
/*
演示RDD与DataFrame与DataSet之间关系以及转换
1. SparkSession私有化构造器，在伴生对象中也没有apply方法，但是伴生对象中有一个内部类Builder，
    其内部getOrCreate方法可以返回SparkSession对象。
2. 查看df里面的数据,show方法默认显示20行数据，在idea中之所以show方法能够显示出表格是通过字符串的各种拼接显示出来的。
3. SparkSession包含SparkContext，SparkContext是SparkSession的一个属性
4. RDD使用toDF、toDS必须导包，import spark.implicits._
    1)注意：spark不是包名，也不是类名，是我们创建的入口对象SparkSession的对象名称
    2)implicits是一个伴生对象，导入他内部的所有方法
    object implicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = SparkSession.this.sqlContext
  }
    3）为什么导包，因为RDD中没有toDF、toDS这些算子，必须通过隐式转换扩展功能后才能使用这些方法。
5. RDD -> DataFrame ->DataSet
  1）当RDD转换成DataFrame时，没有指定列名，也没有使用样例类，则会默认分配列名_1,_2,_3...
  2）当RDD转换成DataSet时，没有指定列名，也没有使用样例类，则会默认分配列名_1,_2,_3...,相当于DataFrame的特例
  3) DataFrame转换为DataSet时，字段名（列名）需要和样例类的属性一一对应，否则报错
  4) DataSet转换成RDD会带上类型，但是DataFrame转换成RDD统一都是Row类型，因为DataFrame只有列名，没有类的概念
    比如 val rdd2: RDD[User] = ds2.rdd   val rdd1: RDD[Row] = df3.rdd

 */

object Test01_Demo {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("China").setMaster("local[*]")
    //创建SparkSQL执行的入口点对象 ， SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate

    //注意：spark不是包名，也不是类名，是我们创建的入口对象SparkSession的对象名称
    import spark.implicits._

    //读取json文件创建DataFrame
    val df: DataFrame = spark.read.json("D:\\workspace_idea\\spark0621\\spark0621exer\\input\\test.json")

    //查看df里面的数据,show方法默认显示20行数据
    df.show(1)
    // def show(): Unit = show(20)

    //1. SQL语法风格：操作DataFrame数据
    // 1）首先需要创建临时表
    df.createOrReplaceTempView("user")
    // 2)使用sql算子执行sql语句，并通过show方法显示
    spark.sql("select * from user").show

    //2.DSL语法风格：操作DataFrame数据
    // 1）直接使用select方法，查询列名，注意列名必须和DataFrame中的列名一致，否则报错
//    df.select("username","age").show() //error
    //org.apache.spark.sql.AnalysisException: cannot resolve '`username`' given input columns: [age, name];
    df.select("name","age").show()

    // 3. RDD与DataFrame与DataSet之间关系以及转换
    //RDD -> DataFrame ->DataSet
    // 创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"banzhang",20),(2,"jingjing",18),(3,"xiaohong",25)))
    //当RDD转换成DataFrame时，没有指定列名，也没有使用样例类，则会默认分配列名_1,_2,_3...
    // 1) RDD --> DataFrame
    val df1: DataFrame = rdd.toDF()
    df1.show()
    //    +---+--------+---+
    //    | _1|      _2| _3|
    //    +---+--------+---+
    //    |  1|banzhang| 20|
    //    |  2|jingjing| 18|
    //    |  3|xiaohong| 25|
    //    +---+--------+---+

    // 2) RDD  --> DataSet
    //当RDD转换成DataSet时，没有指定列名，也没有使用样例类，则会默认分配列名_1,_2,_3...,相当于DataFrame的特例
    val ds: Dataset[(Int, String, Int)] = rdd.toDS()
    ds.show()
    //    +---+--------+---+
    //    | _1|      _2| _3|
    //    +---+--------+---+
    //    |  1|banzhang| 20|
    //    |  2|jingjing| 18|
    //    |  3|xiaohong| 25|
    //    +---+--------+---+

    println("=================================")

    // 3) DataFrame 转换为 DataSet
    // DataFrame转换为DataSet时，字段名（列名）需要和样例类的属性一一对应，否则报错
//    val ds1: Dataset[User] = df1.as[User] //error
    //    ds1.show()
    // org.apache.spark.sql.AnalysisException: cannot resolve '`id`' given input columns: [_1, _2, _3]
    val df2: DataFrame = rdd.toDF("id","name","age")
    val ds2: Dataset[User] = df2.as[User]
    ds2.show

//    +---+--------+---+
//    | id|    name|age|
//    +---+--------+---+
//    |  1|banzhang| 20|
//    |  2|jingjing| 18|
//    |  3|xiaohong| 25|
//    +---+--------+---+

    // 4) DataSet --> DataFrame
    val df3: DataFrame = ds2.toDF

    // 5) DataSet --> RDD
    val rdd2: RDD[User] = ds2.rdd

    // 6) DataFrame -->RDD
    val rdd1: RDD[Row] = df3.rdd

    // 释放资源
    spark.stop()

  }
}

case class User(id:Int,name:String,age:Int)