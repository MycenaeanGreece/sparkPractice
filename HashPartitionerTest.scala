package cn.youe.spark

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by youe on 2017/2/16.
  */
object HashPartitionerTest {
//  def main(args: Array[String]): Unit = {
//    //      val conf = new SparkConf().setAppName("test")
//    //
//    //      val sc = new SparkContext(conf)
//
//    //     sc.textFile("").repartition(2)
////    var key = "hadoop" // hadoop scala java
////
////  val x = key.hashCode()
////  val mod = 3
////
////  val rawMod = x % mod
////  val value = rawMod + (if (rawMod < 0) mod else 0)
////  println(value)
//}

  def main(args: Array[String]): Unit = {

    val sconf = new SparkConf().setAppName("HashPartitionerTest").setMaster("local[2]")

    val sc = new SparkContext(sconf)

    val list = List("hadoop","java","scala","hadoop","java","scala","hadoop","java","scala")
    val template = list.distinct

    val partitioner = new SelfPartitioner(template)

    val rdd = sc.parallelize(list ,3).map((_, 1))

//    val rddNew = rdd.partitionBy(partitioner)

    rdd.foreachPartition(iter=>{
      println(iter.toList.toBuffer)
    })
  }
}

class SelfPartitioner(list: List[String]) extends Partitioner{

  var hashMap01 = new mutable.HashMap[String,Int]()
  var count = 0
  for(item <- list){
    hashMap01 += (item -> count)
    count += 1
  }

  override def numPartitions: Int = list.size

  override def getPartition(key: Any): Int = {
    hashMap01.getOrElse(key.toString,0)
  }
}
