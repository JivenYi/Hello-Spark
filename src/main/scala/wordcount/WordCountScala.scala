package wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)
    val data = Array("spark,kafka,mysql","spark,hadoop,flink","spark,hadoop,hive")
    val wordCount = sc.parallelize(data).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
    wordCount.foreach(println(_))
    sc.stop()
  }

}
