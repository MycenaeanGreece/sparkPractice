package cn.youe.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by youe on 2017/2/17.
  */
object ShareVariableTest {

//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("ShareVariableTest").setMaster("local[4]")
//
//    val context = new SparkContext(conf)
//
//
//    val  rdd = context.parallelize(List(1,2,3,4,5,6,7,8))
//    var sharedCount = 0
//    val rddSharedCount = rdd.map(data => {sharedCount += data;data+10})
//
//
//    println(rddSharedCount.collect().toBuffer)
//    println("the value of variable sharedCount is  " + sharedCount)
//  }

//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("ShareVariableTest").setMaster("local[4]")
//    val context = new SparkContext(conf)
//
//    val  rdd = context.parallelize(List(1,2,3,4,5,6,7,8))
//
//    val broadcastValue = context.broadcast(10)
//
//    val rddBroadCast = rdd.map(data => {data * broadcastValue.value})
//
//    println(rddBroadCast.collect().toBuffer)
//    println(broadcastValue.value)
//  }

//  def main(args: Array[String]): Unit = {
//        val conf = new SparkConf().setAppName("ShareVariableTest").setMaster("local[4]")
//        val context = new SparkContext(conf)
//
//        val  rdd = context.parallelize(List(1,2,3,4,5,6,7,8))
//
//        val accu = context.accumulator(0,"acuumulator")
//       // 陷阱2：accumulater在算子里面不能是 accumulater.value这种方式
//        val rddAccu = rdd.map(data => {accu += data ; data+10})
//      //陷阱1 ： 对rdd多次调用action算子，accumulater就会算几次，
//     // 解决问题: 在算子返回的RDD上，加缓存，缓存需要在action调用之前加
//     // rddAccu.cache()
//        println(rddAccu.collect().toBuffer)
//
//    println(rddAccu.collect().toBuffer)
//    rddAccu.cache()
//    println(rddAccu.collect().toBuffer)
//        println(accu)
//
//  }
}
