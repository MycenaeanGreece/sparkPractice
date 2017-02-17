package cn.youe.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by youe on 2017/2/17.
  */
object CheckPointTest {

  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setAppName("CheckPointTest")

    var sparkContext = new SparkContext(sparkConf)

    sparkContext.setCheckpointDir("")

    val rdd = sparkContext.textFile("").map(_+10)
    rdd.cache()
    //
    rdd.checkpoint()
    //在执行action方法的时候，是从rdd1 -> rddn
    // 执行checkpoint ， 也是从rdd1 -> rddn
    // 为了进行性能优化，在checkpoint的rdd那里，提前调用cache、persist()方法，
    // 从缓存里面的数据写到文件系统（hdfs）
    rdd.collect()
  }
}
