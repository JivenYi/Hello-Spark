package core

import org.apache.spark.{SparkConf, SparkContext}


object LogAppScala {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)
    val fileUrl = this.getClass.getResource("/access.log").getPath
    val logInfoRdd = sc.textFile(fileUrl)
    val LogInfoScalaRdd = logInfoRdd.map(x => (x.split("\t")(1),
      new LogInfoScala(x.split("\t")(0).toLong, x.split("\t")(2).toLong, x.split("\t")(3).toLong)))
    val reduceRdd = LogInfoScalaRdd.reduceByKey((x1, x2) => {
      new LogInfoScala(if (x1.timestamp < x2.timestamp) x1.timestamp else x2.timestamp, x1.upTraffic + x2.upTraffic, x1.downTraffic + x2.downTraffic)
    })
    val mapRdd = reduceRdd.map(x => (x._2, x._1))
    val result = mapRdd.sortByKey(false).take(5)
    result.foreach(println)
    sc.stop()
  }

}
