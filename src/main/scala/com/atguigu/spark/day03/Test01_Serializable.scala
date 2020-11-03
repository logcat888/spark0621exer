package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-24 22:26
 */
/*
序列化：
1.RDD算子相关的操作在Executor端执行，RDD算子外的操作在Driver端执行
2.序列化的理解：对于rdd.foreach(user => println(user))
    1）在Executor端执行，user涉及到从Driver端到Executor端的执行，需要实现序列化
    2）foreach相当于外部函数，user => println(user)相当于内部函数，相当于外部函数的局部变量，user是外部函数传递过来的，
    相当于内部函数访问了外部函数的变量，这里面存在闭包，他怎么知道user这个对象需要做序列化？这里面有一个功能叫闭包检查。
    在执行foreach这个算子时，在真正提交job之前，有一个操作sc.clean(f)，clean（）叫闭包清理或闭包检查。
3.foreach源码：
      def foreach(f: T => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }
4.如何判断当前函数是否有闭包？可以在磁盘上看到会生成一个$annofun的字节码文件
5.源码解读：P81，21分钟。
  1）ClosureCleaner：闭包清理者
  xxxx
6.闭包包含两部分内容：内层函数，以及内层函数访问的外部变量
7.当我们调用RDD行动算子时，如果涉及到闭包，他会做一个闭包检测，检测是否序列化等等

总结：
1.为什么要序列化？
答：因为在Spark程序中，算子相关的操作在Executor上执行，算子之外的代码在Driver端执行，在执行有些算子的时候，
    需要使用到Driver里面定义的数据，这就涉及到了跨进程或者跨节点之间的通讯，所以要求传递给Executor中的
    数据所属的类型必须实现Serializable接口。
2.如何判断是否实现了序列化接口
答：在作业job提交之前，其中有一行代码 val cleanF = sc.clean(f),用于进行闭包检查。之所以叫闭包检查，是因为
    在当前函数的内部访问了外部函数的变量，属于闭包的形式。如果算子的参数是函数的形式，都会存在这种情况。函数
    是实现序列化的，但是函数访问的变量是否实现序列化，需要做检测，如果没有实现序列化，会抛异常。

 */
object Test01_Serializable {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //创建User对象，这些操作在Driver端执行
    val user1 = new User("alice")
    val user2 = new User("bob")

    //创建RDD
    val rdd: RDD[User] = sc.makeRDD(List(user1,user2))
//    rdd.foreach(println)
    /*
    在Executor端执行，user涉及到从Driver端到Executor端的执行
    foreach相当于外部函数，user => println(user)相当于内部函数，相当于外部函数的局部变量，user是外部函数传递过来的，
    相当于内部函数访问了外部函数的变量，这里面存在闭包，他怎么知道user这个对象需要做序列化？这里面有一个功能叫闭包检查。
    在执行foreach这个算子时，在真正提交job之前，有一个操作sc.clean(f)，clean（）叫闭包清理或闭包检查。
    */
    /*
      def foreach(f: T => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }
     */
    rdd.foreach(user => println(user))

    // 3.关闭资源
    sc.stop()
  }
}

class User(var name:String) extends Serializable {
  override def toString: String = s"User($name)"
}