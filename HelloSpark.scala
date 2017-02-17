package cn.youe.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by youe on 2017/2/13.
  */
object HelloSpark {

  def main(args: Array[String]): Unit = {

    var sparkConf = new SparkConf().setAppName("hellospark")

    var sparkContext = new SparkContext(sparkConf)
   //在textFile里面先产生了HadoopRDD-> MapPartitionRDD
    //flatMap里面产生了一个MapPartitionRDD
    // map里面产生了一个MapPartitionRDD
    // reduceByKey里面产生了一个ShuffledRDD
    //saveAsTextFile里面产生了一个MapPartitionRDD
    var rdd = sparkContext.textFile(args(0)).flatMap(_.split(" "))
      .map(x=>(x,1)).reduceByKey(_+_)

    rdd.saveAsTextFile(args(1))

    sparkContext.stop()
  }

}
