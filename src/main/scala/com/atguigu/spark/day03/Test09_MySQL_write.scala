package com.atguigu.spark.day03

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-26 10:05
 */
/*
向MySQL数据库中写入数据
1. JDBC连接数据库的六大步骤：
    1）注册驱动；2）获取连接；3）创建数据库操作对象PrepareStatement；4）执行SQL；5）处理结果集；6）关闭连接
2. 数据库连接4要素: 1)获取驱动所在位置（全类名），通过反射获取Driver对象；2）数据库连接地址；3）用户名；4）密码
3. 添加数据库连接相关依赖
4. 数据库连接是一个重要级的对象，每次创建都需要消耗大量的资源。关于连接池，常量池其实都是缓存技术，
5. 使用foreachPartition 以分区为单位将分区内的数据批处理写入数据库
 */
object Test09_MySQL_write {
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

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("bangzhang",30),("jingjing",18)))

 /*   //注意：在循环体中创建连接对象，每次遍历出RDD中的一个元素，都要创建一个连接对象，效率低，不推荐使用
     即便使用连接池，效率也是非常低的，因为只是省略了每次自己创建连接的时间，但是从数据库连接池中获取已经缓存创建好的连接也是要
     花费时间的。 最好是获取一次连接，批处理数据，不再是这样一个元素，一个元素的处理。
    rdd.foreach{
      case (name,age) =>{
        // 1）注册驱动；
        Class.forName(driver)
        // 2）获取连接；
        val conn: Connection = DriverManager.getConnection(url,username,password)
        //声明数据库操作的SQL语句，只写name，age，因为id为自增长列，可以不用管
        var sql:String = "insert into user(name,age) values(?,?)"
        // 3）创建数据库操作对象PrepareStatement；可以避免sql注入
        val ps: PreparedStatement = conn.prepareStatement(sql)
        // 给参数赋值
        ps.setString(1,name)
        ps.setInt(2,age)
        // 4）执行SQL；
        ps.executeUpdate()
        // 5）处理结果集 // 执行的是插入操作，不存在结果集
        // 6）关闭连接
        ps.close()
        conn.close()
      }
    }
   */
    /*

    注意： 下面这段代码，需要让PreparedStatement实现序列化，但是PreparedStatement不是我们自己定义的类型，
    没有办法实现序列化。并且也不能通过反编译的方式修改源代码让连接实现序列化，因为对连接实现序列化是非常危险的。

    为什么需要让ps对象实现序列化，因为ps数据库连接对象是在Driver端创建的，而执行sql是在Executor端执行的，
    因为使用到了外部变量，需要做闭关检查，看是否实现序列化。

    // 优化方式
    // 1）注册驱动；
    Class.forName(driver)
    // 2）获取连接；获取数据库连接的方式还可以进一步优化为从数据库连接池中获取
    val conn: Connection = DriverManager.getConnection(url,username,password)
    //声明数据库操作的SQL语句，只写name，age，因为id为自增长列，可以不用管
    var sql:String = "insert into user(name,age) values(?,?)"
    // 3）创建数据库操作对象PrepareStatement；可以避免sql注入
    val ps: PreparedStatement = conn.prepareStatement(sql)

    rdd.foreach{
      case (name,age) =>{
        // 给参数赋值
        ps.setString(1,name)
        ps.setInt(2,age)
        // 4）执行SQL；
        ps.executeUpdate()
      }
    }
    // 5）处理结果集 // 执行的是插入操作，不存在结果集
    // 6）关闭连接
    ps.close()
    conn.close()
    */

    //最终优化：这样数据库连接对象conn，ps对象，都是在Executor上创建的，对一个分区整体的数据进行批处理，
    // 不涉及从Driver端到Executor端的传输。
    // 优化点是：一个分区创建一个连接，以前是一个元素创建一个连接，性能大大滴提升
    // map ===> mapPartition 以分区为单位，适合批处理
    rdd.foreachPartition(
      // datas是RDD的一个分区的数据
      datas => {
        // 1）注册驱动；
        Class.forName(driver)
        // 2）获取连接；
        val conn: Connection = DriverManager.getConnection(url,username,password)
        //声明数据库操作的SQL语句，只写name，age，因为id为自增长列，可以不用管
        var sql:String = "insert into user(name,age) values(?,?)"
        // 3）创建数据库操作对象PrepareStatement；可以避免sql注入
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //对当前分区内的数据进行遍历，该foreach是集合中的方法，不是算子
        datas.foreach{
          case (name,age) => {
            // 给参数赋值
            ps.setString(1,name)
            ps.setInt(2,age)
            // 4）执行SQL；
            ps.executeUpdate()
            // 5）处理结果集 // 执行的是插入操作，不存在结果集
          }
        }
        // 6）关闭连接
        ps.close()
        conn.close()
      }
    )


    // 3.关闭资源
    sc.stop()
  }

}
