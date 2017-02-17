package cn.youe.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by youe on 2017/2/16.
  */
object SharedVariable {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setAppName("SharedVariable").setMaster("local[4]")
    var sparkContext = new SparkContext(sparkConf)
    var  rdd = sparkContext.parallelize(List(1,2,3,4,5,6,7,8))
    var count = 0
    rdd.cache()
    rdd.foreach(iter => {
       count += iter
    })

    println(count)
    sparkContext.stop()
  }
}
