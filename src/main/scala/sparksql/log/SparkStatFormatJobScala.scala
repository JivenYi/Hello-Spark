package sparksql.log

import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗:抽取出我们需要的指定列的数据
  */
object SparkStatFormatJobScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
    val accsee = spark.sparkContext.textFile(this.getClass.getResource("/mooc_access.log").getPath)
    //    accsee.take(5).foreach(println(_))
    accsee.map(line => {
      val splits = line.split(" ")

      val ip = splits(0)
      //[10/Nov/2016:00:01:02 +0800]=>yyyy-MM-dd HH:mm:ss
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replaceAll("\"", "")
      val traffic = splits(9)
      //      (ip,DateUtilsScala.parse(time),url,traffic)
      DateUtilsScala.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).coalesce(1).saveAsTextFile("hdfs://192.168.195.100:9000/mooc/output/")


    spark.stop()

  }


}
