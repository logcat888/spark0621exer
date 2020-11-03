package com.atguigu.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author chenhuiup
 * @create 2020-09-29 18:06
 */
/*
自定义UDAF（弱类型  主要应用在SQL风格的DF查询）
已过时，不用，太麻烦
 */
object Test05_UDAF {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("China").setMaster("local[*]")
    //创建SparkSQL执行的入口点对象 ， SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //注意：spark不是包名，也不是类名，是我们创建的入口对象SparkSession的对象名称
    import spark.implicits._

    //创建自定义函数对象
//    val myAvg = new MyAvg
    //注册自定义函数
    spark.udf.register("myAvg1",new MyAvg)

    //读取Json文件，创建DF
    val df: DataFrame = spark.read.json("D:\\workspace_idea\\spark0621\\spark0621exer\\input\\test.json")

    //创建临时视图
    df.createOrReplaceTempView("user")

    //使用聚合函数进行查询
    spark.sql("select avg(age) from user").show()
    spark.sql("select myAvg1(age) from user").show()

    //关闭资源
    spark.close()

  }
}

//自定义UDAF函数(弱类型)
/*
@deprecated("Aggregator[IN, BUF, OUT] should now be registered as a UDF" +
  " via the functions.udaf(agg) method.", "3.0.0")
 */
// 该方法已经过时
// 聚合函数需要指定三个数据类型：1）输入数据类型；2）缓存数据类型；3）输出数据类型
class MyAvg extends UserDefinedAggregateFunction{
  //聚合函数的输入数据的类型
  override def inputSchema: StructType = {
    // 1）需要返回StructType类型，使用StructType伴生对象，其构造器需要传入数组，且元素为StructField类型
    // 2）IntegerType就是Int类型，只是这是org.apache.spark.sql.types自己定义的类型
    // 3）age 相当于给类型IntegerType起个名字，叫什么都可以
    // 4）IntegerType是二维表格中的数据类型
    // 5）StructType可以指定多个数据类型，因为有可能一次传入多个数据，所以需要将多个类型放到数组中
    // 6）一个StructType类型可以包含多个字段类型
    // case class StructType(fields: Array[StructField]) extends DataType with Seq[StructField]
    // class IntegerType private() extends IntegralType
    StructType(Array(StructField("age",IntegerType)))
  }

  //缓存数据的类型：缓存的意思是指求平均数时，需要把所有元素遍历一遍，因此将每次累加的变量放到内存中存起来
  override def bufferSchema: StructType = {
    // 两个参数：一个数统计总年龄；一个统计总人数
    // 两个数据类型
    StructType(Array(StructField("sum",LongType),StructField("count",LongType)))
  }

  //聚合函数返回的数据类型：聚合函数多进一出，返回一个值就可
  override def dataType: DataType = DoubleType

  //稳定性  默认不处理，直接返回true    相同输入是否会得到相同的输出
  // 意思就是给两次相同的数，两次的计算结果相同，很稳定
  override def deterministic: Boolean = true

  //初始化  缓存设置到初始状态，类似于累加器的reset（）
  // 总年龄和总人数组成一行数据
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //abstract class MutableAggregationBuffer extends Row
    //让缓存中年龄总和归0,一行数据的第一个
    buffer(0) = 0L
    //让缓存中总人数归0，一行数据的第二个
    buffer(1) = 0L
  }

  // 更新缓存数据，第一个参数缓存数据，第二个参数是一行数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    // 1）buffer.getAs[Long](0) //获取缓存中的第一个数据，这种方式类似于强制类型转换
    // 2）buffer.getLong(0) 获取缓存中的第一个数据，不能写成buffer(0) 因为他不知道缓存中第一个数据是什么类型，
    //    可以赋值，但是不能相加，只有确定类型后才能使用
    // 3）input.getInt(0) 获取一行数据中的第一列的值，必须明确一列数据的类型，因为不知道一行数据中第一列是什么类型
    // 4) !buffer.isNullAt(0)使用这个的目的就是判断缓存是否已经初始化了，可以增加程序的稳定性和健壮性
    if (!buffer.isNullAt(0)){
      buffer(0) = buffer.getLong(0) + input.getInt(0)
      buffer(1) = buffer.getLong(1) + 1L
    }
  }

  //分区间的合并，就是各个缓存中数据的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算逻辑
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}