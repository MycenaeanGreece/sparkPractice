package cn.youe.spark

import org.apache.spark.{SparkConf, SparkContext}

//object  Context {
//  implicit  val girl = new Ordering[Girl]{
//    override def compare(x: Girl, y: Girl): Int = {
//      if(y.faceValue == x.faceValue){
//              y.age - x.age
//            }else {
//              x.faceValue - y.faceValue
//            }
//    }
//  }
//}

/**
  * Created by youe on 2017/2/16.
  */
object SelfSorted {
  //Oredered  Ordering
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setAppName("SelfSorted").setMaster("local[2]")

    val sc = new SparkContext(sconf)

    val data = List(("lili", 95, 22) , ("hanmeimei" , 90, 27), ("wangfang", 90,25))
//    import Context._
    val rdd = sc.parallelize(data).sortBy(data => {
    new Girl(data._1,data._2 ,data._3)
    },false)

    println(rdd.collect().toBuffer)


  }

}
// 在自定义排序当中，自定义类需要加上case
 case class Girl(name: String, faceValue:Int , age: Int) extends Ordered[Girl] with Serializable {
  override def compare(that: Girl): Int = {
    if(this.faceValue == that.faceValue){
      that.age - this.age
    }else {
      this.faceValue - that.faceValue
    }
  }
}

//case class Girl(name:String, faceValue :Int, age :Int) extends Serializable