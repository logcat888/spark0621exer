package com.atguigu.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn, functions}
import org.apache.spark.sql.expressions.Aggregator

/**
 * @author chenhuiup
 * @create 2020-09-29 19:28
 */
/*
自定义UDAF（强类型  主要应用在DSL风格的DS查询）
 */
object Test06_UDAF {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("China").setMaster("local[*]")
    //创建SparkSQL执行的入口点对象 ， SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //注意：spark不是包名，也不是类名，是我们创建的入口对象SparkSession的对象名称
    import spark.implicits._

    // 创建自定义函数对象
    val avgNew: MyAvgNew = new MyAvgNew

    /*
     注意：
        1）通过继承Aggregator创建的UDAF函数，不能直接将函数对象当做参数传递
        2）如果是自定义UDAF的强类型，没有办法应用SQL风格DF的查询，因为没有将df中的每一列对应为对象的属性，每一行对应样例类对象，
          所以自定义UDAF的强类型函数无法识别。有两种使用方式
    // 注册自定义函数
//    spark.udf.register("myAvgNew",avgNew) //error
    //读取Json文件，创建DF
    val df: DataFrame = spark.read.json("D:\\workspace_idea\\spark0621\\spark0621exer\\input\\test.json")
    //创建临时视图
    df.createOrReplaceTempView("user")
    //使用聚合函数进行查询
    spark.sql("select avg(age) from user").show()
    spark.sql("select myAvgNew(age) from user").show()
     */

    //读取Json文件，创建DF
    val df: DataFrame = spark.read.json("D:\\workspace_idea\\spark0621\\spark0621exer\\input\\test.json")

    // 自定义UDAF函数（强类型）使用方案一：DSL语法风格，将df转化为ds从而形成对象映射
    // 1）将df转换为ds，从而将一行数据映射为一个对象
    val ds: Dataset[User06] = df.as[User06]

    // 2）将自定义函数对象转换为查询列
    val col: TypedColumn[User06, Double] = avgNew.toColumn

    // 3）select函数只能放一列，列名  或者 TypedColumn类型对象。所以select相当于把一行行的对象传递给自定义UDAF函数对象执行聚合
    // 在进行查询的时候，会将插叙处理的记录（User06类型）交给自定义的函数进行处理
    ds.select(col).show()
//    {"name":"wangwu","age":19} json文件 如果19加上双引号，就是字符串类型，不能转换为bigint类型。 但是注册使用时，不加“”也行
//    org.apache.spark.sql.AnalysisException: Cannot up cast `age` from string to bigint.
//    +-------------------------------------------+
//    |MyAvgNew(com.atguigu.spark.sparksql.User06)|
//      +-------------------------------------------+
//    |                                       19.0|
//      +-------------------------------------------+

    // 自定义UDAF函数（强类型）使用方案二：SQL语法风格，注册使用，df操作

    // 1)注册UDAF
    spark.udf.register("myAvg2",functions.udaf(new MyAvgNew2))

    // 2) 创建DataFrame临时视图
    df.createOrReplaceTempView("user")

    // 3) 调用自定义UDAF函数
    spark.sql("select myAvg2(age) from user").show()
//    +--------------+
//    |myavgnew2(age)|
//      +--------------+
//    |          19.0|
//      +--------------+

    spark.sql("select avg(age) from user").show()
//    +--------+
//    |avg(age)|
//      +--------+
//    |    19.0|
//      +--------+

    //关闭资源
    spark.close()
  }

}

//输入类型的样例类
//case class User06(name:String,age:Short) // Cannot up cast `age` from string to smallint.
case class User06(name:String,age:Long) // 从外部文件读取时，由于不知道数字类型，默认都是bigint，他只能和Long进行转换

//缓存数据的类型，因为是需要一直变化，所以定义为var
case class AgeBuffer(var sum:Long,var count:Long)


//自定义UDAF函数（强类型）
// 注意Aggregator导包千万别导错了，是这个包import org.apache.spark.sql.expressions.Aggregator
/*
1. Aggregator泛型参数解读  abstract class Aggregator[-IN, BUF, OUT] extends Serializable
@tparam IN The input type for the aggregation. 输入数据类型
@tparam BUF The type of the intermediate value of the reduction. 缓存数据类型
@tparam OUT The type of the final output result. 输出结果数据类型

2. 强类型与弱类型的区别。
  弱类型在传递一行数据时，不清楚这一行数据对应着哪个对象。但是当强类型在查询一行数据时，清楚这一行数据对应着哪个对象，因为
  在创建DataSet时指定了一行数据对应的类，所以每一行数据都是该类的对象，每一列是对象的属性。所以在输入数据时，不像弱类型需要
  使用StrutType指定一行数据中某一列的类型，因为DataSet是强类型的，在输入数据时某一列是什么类型都已经非常明确了，所以可以
  定义一个输入数据的样例类，表示输入数据。
 */
// 供DSL语法风格使用
class MyAvgNew extends Aggregator[User06,AgeBuffer,Double] {

  //对缓存数据进行初始化
  override def zero: AgeBuffer = AgeBuffer(0L,0L)

  // 对当前分区内数据进行聚合
  override def reduce(b: AgeBuffer, a: User06): AgeBuffer = {
    b.sum += a.age
    b.count += 1
    b
  }

  //分区间合并
  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  //返回计算结果
  override def finish(reduction: AgeBuffer): Double = {
    reduction.sum.toDouble /reduction.count
  }

  // DataSet的编码以及解码器，用于进行序列化，固定写法
  // 用户自定义Ref类型 product   系统值类型，根据具体类型进行选择
  override def bufferEncoder: Encoder[AgeBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}


// 供SQL语法风格使用
// 注意：Aggregator的输入泛型为输入列的类型
class MyAvgNew2 extends Aggregator[Long,AgeBuffer,Double] {

  //对缓存数据进行初始化
  override def zero: AgeBuffer = AgeBuffer(0L,0L)

  // 对当前分区内数据进行聚合
  override def reduce(b: AgeBuffer, age: Long): AgeBuffer = {
    b.sum += age
    b.count += 1
    b
  }

  //分区间合并
  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  //返回计算结果
  override def finish(reduction: AgeBuffer): Double = {
    reduction.sum.toDouble /reduction.count
  }

  // DataSet的编码以及解码器，用于进行序列化，固定写法
  // 用户自定义Ref类型 product   系统值类型，根据具体类型进行选择
  override def bufferEncoder: Encoder[AgeBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}