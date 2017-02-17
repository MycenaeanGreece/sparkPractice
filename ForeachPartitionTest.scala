package cn.youe.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by youe on 2017/2/13.
  */
object ForeachPartitionTest {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setAppName("ForeachPartitionTest").setMaster("local[3]")

    var sparkContext = new SparkContext(sparkConf)

    val rdd1 = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)

    rdd1.foreach(x => {print(x)})

    sparkContext.stop()
  }
}
