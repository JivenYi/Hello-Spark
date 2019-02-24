package core

import org.apache.spark.{SparkConf, SparkContext}

object AvgAgeScala {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)
    //(id,age)
    val data = Array("1,12","2,21","3,45","4,60","5,34")
    val ageData = sc.parallelize(data)
    //(id,age) => (age,1)
    val age = ageData.map(x=>(x.split(",")(1).toInt,1))
    val result = age.reduce((x,y)=>(x._1+y._1,x._2+y._2))
    val avgAge = result._1/result._2
    println(avgAge)
    sc.stop()
  }

}
