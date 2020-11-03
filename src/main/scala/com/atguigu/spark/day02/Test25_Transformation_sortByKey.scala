package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-24 17:54
 */
/*
转换算子 -- sortByKey
--按照key对RDD中的元素进行排序
1. 使用sortByKey是针对KV类型，且按照key进行排序，如果一个key有多个value，则按照集合中的顺序
2. 如果key是自定义类型，则必须混入特质Ordered或Ordering，自定义比较规则

-- 自定义类自定义比较规则
0. 对于Int、String等基本类型，scala已经实现了比较规则，但是自定义类型需要自己实现比较规则，或自定义比较规则
1.如果key为自定义类型，要求必须混入Ordered特质,且Ordered必须指定泛型
2. Ordered特质继承java中的Comparable接口，需要实现compareTo方法
  trait Ordered[A] extends scala.Any with java.lang.Comparable[A]
3. Ordering特质继承java中的Comparator接口，需要实现Compare方法
  trait Ordering[T] extends java.lang.Object with java.util.Comparator[T]
4. 必须继承Serializable特质，实现序列化，否则会抛异常 java.io.NotSerializableException
5. 必须要重写toString方法，以便打印输出，否则打印出的是地址值

 */
object Test25_Transformation_sortByKey {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd"),(1,"cc"),(1,"ff")))

    //按照key对rdd中的元素进行排序
    val newRDD: RDD[(Int, String)] = rdd.sortByKey()
    newRDD.collect().foreach(println)
    /*
    (1,dd)
    (1,cc)
    (1,ff)
    (2,bb)
    (3,aa)
    (6,cc)
     */
    println("==================")
    //降序
    rdd.sortByKey(false).collect().foreach(println)
    /*
    (6,cc)
    (3,aa)
    (2,bb)
    (1,dd)
    (1,cc)
    (1,ff)
     */

    //如果key为自定义类型，要求必须混入Ordered特质
    val stdList: List[(Student, Int)] = List(
      (new Student("jingjing", 18), 2),
      (new Student("bangzhang", 18), 2),
      (new Student("jingjing", 20), 2),
      (new Student("zhangfei", 18), 3),
      (new Student("jingjing", 19), 2),
      (new Student("banzhang", 15), 5)
    )
    val rdd1: RDD[(Student, Int)] = sc.makeRDD(stdList)

    rdd1.sortByKey().collect().foreach(println)
    /*
    (bangzhang--18,2)
    (banzhang--15,5)
    (jingjing--18,2)
    (jingjing--19,2)
    (jingjing--20,2)
    (zhangfei--18,3)
     */

    // 3.关闭资源
    sc.stop()
  }

}
/*
0. 对于Int、String等基本类型，scala已经实现了比较规则，但是自定义类型需要自己实现比较规则，或自定义比较规则
1.如果key为自定义类型，要求必须混入Ordered特质,且Ordered必须指定泛型
2. Ordered特质继承java中的Comparable接口，需要实现compareTo方法
  trait Ordered[A] extends scala.Any with java.lang.Comparable[A]
3. Ordering特质继承java中的Comparator接口，需要实现Compare方法
  trait Ordering[T] extends java.lang.Object with java.util.Comparator[T]
4. 必须继承Serializable特质，实现序列化，否则会抛异常 java.io.NotSerializableException
5. 必须要重写toString方法，以便打印输出，否则打印出的是地址值
 */
class Student(val name:String,val age:Int) extends Ordered[Student] with Serializable {

  //指定比较规则
  override def compare(that: Student): Int = {
    //先按照名称升序排序，如果名称相同的话，再按照年龄升序排序
    var res = this.name.compareTo(that.name)
    if (res != 0) res else this.age.compareTo(that.age)
  }

  override def toString: String = name + "--" + age
}


