package com.atguigu.spark.day03

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-26 9:10
 */
/*
从MySQL数据库中读取数据
1. 创建的RDD常见两种方式：1）集合，makeRDD；2）外部文件textFile，这两种方式似乎都不能从mysql中获取数据，只能通过JDBC的方式获取数据。
2. RDD是一个抽象类，找他的具体实现类发现有JdbcRDD
3. JdbcRDD参数解析：
    sc: SparkContext, Spark程序执行的入口，上下文对象
    getConnection: () => Connection,  获取数据库连接
    sql: String, 执行SQL语句，需要预留两个占位符 //比如select title, author from books where ? <= id and id <= ?
    lowerBound: Long, 查询的起始位置
    upperBound: Long, 查询的结束位置
    numPartitions: Int, 分区数  //根据sql、起始位置、结束位置以及分区数，就可以知道每个分区放入多少数据。
    mapRow: (ResultSet) => T 对结果集的处理
4. JDBC连接数据库的六大步骤：
    1）注册驱动；2）获取连接；3）创建数据库操作对象PrepareStatement；4）执行SQL；5）处理结果集；6）关闭连接
5. 数据库连接4要素: 1)获取驱动所在位置（全类名），通过反射获取Driver对象；2）数据库连接地址；3）用户名；4）密码
6. 添加数据库连接相关依赖
7. user表结构：  id :int (自增长列), name:varchar  , age:int
 */
object Test08_MySQL_read {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //数据库连接4要素
    var driver = "com.mysql.jdbc.Driver" //获取驱动所在位置
    // 1）jdbc:mysql：// jdbc的mysql连接协议； 2）hadoop102:3306 ，socket； 3）test为连接的数据库的库名
    var url = "jdbc:mysql://hadoop102:3306/test" //数据库连接地址
    var username = "root"
    var password = "123456"

    //预编译语句，?为占位符
    var sql :String = "select * from user where id >= ? and id <= ?"

    val newRDD: JdbcRDD[(Int, String, Int)] = new JdbcRDD(
      sc,
      () => {
        //注册驱动
        Class.forName(driver)
        //获取连接,返回值为连接对象
        DriverManager.getConnection(url, username, password)
      },
      sql,
      1, //起始位置，第一个占位符
      20, //结束位置，第二个占位符
      2, // 分区数
      rs => (rs.getInt(1), rs.getString(2), rs.getInt(3)) //获取第一列id，第二列name，第三列age
    )
    newRDD.collect().foreach(println)

    // 3.关闭资源
    sc.stop()
  }

}
