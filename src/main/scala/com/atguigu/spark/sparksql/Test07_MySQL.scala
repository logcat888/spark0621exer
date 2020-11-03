package com.atguigu.spark.sparksql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SaveMode, SparkSession}

/**
 * @author chenhuiup
 * @create 2020-09-29 23:13
 */
/*
通过jdbc对MySQL进行读写操作
1. 数据库主键的作用就是标记唯一性，一般主键会选用非业务的数据作为主键，比如身份证号尽管唯一，但是作为主键不符合开发规范。
2. 在mysql中主键设置为自增长主键，当删除表中的一些数据后（比如删除 5,6,7），再向表中插入数据，主键号会接着删之前的号继续
    往下排（8,9）。但是如果mysql服务重新启动，删除再插入数据，会按照（5,6,7）追加。  主键号就是唯一性标记，用于索引使用，
    没有太多业务数据。
3. 向mysql中写入数据，mode必须指定为append，否则报错。

 */
object Test07_MySQL {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("China").setMaster("local[*]")
    //创建SparkSQL执行的入口点对象 ， SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //注意：spark不是包名，也不是类名，是我们创建的入口对象SparkSession的对象名称
    import spark.implicits._

    //从MySQL数据库中读取数据  方式1：通过option
    /*
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall") // 连接mysql数据库中的表
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info") //指定连接的表
      .load()
    df.show()
     */

    /*
    //从MySQL数据库中读取数据  方式2：options(Map（）)
    val df: DataFrame = spark.read.format("jdbc")
      .options(Map(
        //直接在url中加入用户名和密码
        "url" -> "jdbc:mysql://hadoop102:3306/gmall?user=root&password=123456",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "user_info"
      )).load()
    df.show()
     */


    //从MySQL数据库中读取数据  方式3：使用jdbc直接读取
//     val df: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop102:3306/gmall?user=root&password=123456","user_info",null)
    val props: Properties = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","12346")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop102:3306/gmall","user_info",props)
    df.show()


    //------------------------------------------------------//
    //向MySQL数据库中写入数据
    val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("banzhang",18),("jingjing",20)))
    //将rdd转换为DF
//    val df1: DataFrame = rdd.toDF()
//    df1.write.format("jdbc").mode("append").save()

    //将rdd转换为DS
    val ds: Dataset[(String, Int)] = rdd.toDS()
    ds.write.format("jdbc")
        .option("url","jdbc:mysql://hadoop102:3306/gamll")
        .option("driver","com.mysql.jdbc.Driver")
        .option("user","root")
        .option("password","123456")
        .option(JDBCOptions.JDBC_TABLE_NAME,"user_info") // 可以使用JDBCOptions的属性，指定JDBC四要素，避免写错
        .mode("append")
//        .mode(SaveMode.Append)  // 可以使用SaveMode伴生对象的枚举对象，指定mode
        .save()

    //关闭资源
    spark.close()
  }

}


case class User2(name:String,age:Int)