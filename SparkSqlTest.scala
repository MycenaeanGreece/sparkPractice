package cn.youe.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by youe on 2017/2/17.
  */
object SparkSqlTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlTest").setMaster("local")
    val sparkContext = new SparkContext(conf)
    // sparkcontext传递给它
    val sqlContext = new SQLContext(sparkContext)

    val rdd = sparkContext.textFile("E:\\data\\person_t.txt").map(_.split(" "))
      .map(x =>  Person(x(0).toInt,x(1).toString,x(2).toInt))

    println(rdd.collect().toBuffer)
    //需要进行隐式导入
    import sqlContext.implicits._
    val rddDF = rdd.toDF

    rddDF.registerTempTable("t_person")
    sqlContext.sql("select * from t_person").show
//    sqlContext.sql("desc t_person").show
    sparkContext.stop()
  }
}
//定义成样例类这种形式
case class Person(id:Int, name:String , age: Int)
