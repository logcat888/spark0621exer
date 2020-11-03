package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkConf, SparkContext}

/**
 * @author chenhuiup
 * @create 2020-09-25 7:23
 */
/*
查看血缘关系和依赖关系：
1. 即便没有触发行动算子，RDD之间的血缘关系也是有的，可以通过toDebugString查看
2. 整个程序叫一个应用，应用下面每触发一次行动算子会提交一个job，job下又划分为多个stage（阶段），
    stage划分是根据当前程序执行过程中是否有Shuffle，只不过有专门的概念叫依赖关系，RDD与RDD之间的
    依赖关系，和Shuffle之间的依赖关系是不一样的。有Shuffle会开一个新的阶段。
3. 血缘关系：是整体概念，可以理解为完成我们的需求，所有RDD之间的关系，不是单个的，我的上级是谁。
    通过血缘关系可以知道当前RDD是从哪里来的，可以追溯到根。
4. 依赖关系：是个体概念，描述的是针对于当前RDD与上级RDD之间的关系。即子对父的依赖。
5. RDD五大特性：一组分区，分区计算函数，依赖关系（获得的就是当前RDD与上级RDD之间的关系），分区器（针对于KV类型），优先位置。
6. 依赖关系分为宽依赖和窄依赖
7. OneToOneDependency也是一个类，说明依赖关系也是一个类型，继承于NarrowDependency（窄依赖），NarrowDependency下有3个子类，
    分别为PruneDependency、RangeDependency和OneToOneDependency
    class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
      override def getParents(partitionId: Int): List[Int] = List(partitionId)
    }
    abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T]
8. 如何区分宽依赖和窄依赖，记住如果不是窄就是宽。也就是说如果当前依赖没有继承NarrowDependency，那么他就是宽依赖。
    Dependcy是宽依赖，其下有两个子类ShuffleDependency和NarrowDependency。一般我们说宽依赖指的就是ShuffleDependency。
9. 血缘关系与依赖关系的应用情况：
    1）血缘关系：血缘关系把整个RDD的计算过程都记录下来，主要是spark底层用于数据的恢复，比如某个分区的数据丢失，就可以通过
      血缘关系知道这个分区的数据从哪个RDD来的，这样就可以一级一级的往上找，从而spark框架做到容错机制。
    2）依赖关系：一般应用下面有多个job，一个job有多个阶段，一个stage分为多个task，阶段就是通过依赖关系来划分。
10. 关于宽依赖与窄依赖的理解：
    1）窄依赖：一个RDD为了提高并行度，通常将数据分为多个分区，交给多个Executor执行。当一个分区的数据只交给一个分区处理，
       就是窄依赖，没有经过Shuffle过程，将一个分区的数据交给多个分区。比如一个rdd有3个分区，分区内的数据分别为【0号（1,2），
       1号（3,4），2号（5,6）】，当分区缩减为2个，没有经过Shuffle时【0号（1,2），1号（3,4,5,6）】。即数据在父RDD中在
       一个分区，在子RDD中还在同一个分区并没有拆散他们。
    2）宽依赖：父RDD中一个分区的数据，在子RDD中被多个分区处理，即数据经过Shuffle被打乱重组。底层存在Shuffle的就是宽依赖。根据宽依赖
      划分job的阶段。
11. 查看RDD的血缘关系以及依赖关系
	1）血缘关系：toDebugString
	2）依赖关系：dependencies
		>窄依赖：父RDD一个分区中的数据，还是交给子RDD的一个分区处理
		>宽依赖：父RDD一个分区中的数据，交给子RDD的多个分区处理
 */
object Test03_Lineage {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"), 2)

    //查看血缘关系
    println(rdd.toDebugString)
    //查看依赖关系:返回值是一个集合，因为一个RDD可能有多个RDD转换而来
    val dependencies: Seq[Dependency[_]] = rdd.dependencies
    //(2) ParallelCollectionRDD[0] at makeRDD at Test03_Lineage.scala:23 []
    println(dependencies)
    // 当前RDD为最顶层RDD，由集合创建，没有依赖关系
    //List()
    println("---------------------------------")

    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    println(flatMapRDD.toDebugString)
    // (2)代表有几个分区， [0]  [1] 代表RDD的顺序
//    (2) MapPartitionsRDD[1] at flatMap at Test03_Lineage.scala:30 []
//    |  ParallelCollectionRDD[0] at makeRDD at Test03_Lineage.scala:23 []
    println(flatMapRDD.dependencies)
    //List(org.apache.spark.OneToOneDependency@51d143a1)
    println("---------------------------------")

    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_,1))
    println(mapRDD.toDebugString)
//    (2) MapPartitionsRDD[2] at map at Test03_Lineage.scala:37 []
//    |  MapPartitionsRDD[1] at flatMap at Test03_Lineage.scala:30 []
//    |  ParallelCollectionRDD[0] at makeRDD at Test03_Lineage.scala:23 []
    println(mapRDD.dependencies)
    //List(org.apache.spark.OneToOneDependency@2c6ee758)
    println("---------------------------------")

    val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resRDD.toDebugString)
    //注意：分阶段了，idea上看效果更明显
//    (2) ShuffledRDD[3] at reduceByKey at Test03_Lineage.scala:44 []
    //    +-(2) MapPartitionsRDD[2] at map at Test03_Lineage.scala:37 []
    //    |  MapPartitionsRDD[1] at flatMap at Test03_Lineage.scala:30 []
    //    |  ParallelCollectionRDD[0] at makeRDD at Test03_Lineage.scala:23 []
    println(resRDD.dependencies)
    //List(org.apache.spark.ShuffleDependency@4a14c44f)
    println("---------------------------------")

    val resRDD1: RDD[(String, Int)] = resRDD.map(t => (t._1,t._2 + 1))
    println(resRDD1.toDebugString)
//    (2) MapPartitionsRDD[4] at map at Test03_Lineage.scala:52 []
//    |  ShuffledRDD[3] at reduceByKey at Test03_Lineage.scala:44 []
    //    +-(2) MapPartitionsRDD[2] at map at Test03_Lineage.scala:37 []
    //    |  MapPartitionsRDD[1] at flatMap at Test03_Lineage.scala:30 []
    //    |  ParallelCollectionRDD[0] at makeRDD at Test03_Lineage.scala:23 []
    println(resRDD1.dependencies)
    //List(org.apache.spark.OneToOneDependency@6794ac0b)
    println("---------------------------------")

    // 3.关闭资源
    sc.stop()
  }
}
