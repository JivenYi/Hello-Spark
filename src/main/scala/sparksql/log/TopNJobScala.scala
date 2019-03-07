package sparksql.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
/**
  * TopN统计分析
  */
object TopNJobScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName)
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
      .getOrCreate()
    val accessDF = spark.read.format("parquet").load("hdfs://192.168.195.100:9000/mooc/clean/")

//    accessDF.printSchema()
//    accessDF.show()
    //最受欢迎视频topN课程
//    videoAccessTopNStat(spark,accessDF)
  //按照地市进行统计topN课程
    cityAccessTopNStat(spark,accessDF)

    spark.stop()
  }

  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {
    import spark.implicits._
//    val videoAccessTopNDF = accessDF.filter($"hour"==="2016111000" && $"cmsType"==="video").groupBy($"cmsId",$"hour")
//      .agg(count($"cmsId").alias("times")).orderBy($"times".desc)
//    videoAccessTopNDF.show()
    accessDF.createOrReplaceTempView("access")
    val videoAccessTopNDF = spark.sql("select cmsId,hour,count(cmsId) as times from access where hour = '2016111000' and cmsType='video'" +
      "group by cmsId,hour order by times desc")
    //将统计结果写到mysql数据库中

    try {
      videoAccessTopNDF.foreachPartition(x => {
        var list = new ListBuffer[HourVideoAccessStat]()
        x.foreach(info => {
          val hour = info.getAs[String]("hour")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          list.append(HourVideoAccessStat(hour, cmsId, times))
        })
        StatDAO.insertHourVideoAccessTopN(list)
      })
    } catch {
      case e:Exception =>e.printStackTrace()
    }
  }


  def cityAccessTopNStat(spark: SparkSession,accessDF:DataFrame): Unit ={
    accessDF.createOrReplaceTempView("access")
    val cityAccessTopNDF = spark.sql("select city,cmsId,hour,count(cmsId) as times from access where hour = '2016111000' and cmsType='video'" +
      "group by city,hour,cmsId order by times desc")
    cityAccessTopNDF.createOrReplaceTempView("cityAccess")
    val top3DF = spark.sql("select * from (select city,cmsId,hour,times,row_number() over (partition by city order by times desc) as rank from cityAccess) where rank<=3  ")

    try {
      top3DF.foreachPartition(x => {
        var list = new ListBuffer[HourCityVideoAccessStat]()
        x.foreach(info => {
          val hour = info.getAs[String]("hour")
          val city = info.getAs[String]("city")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          val rank = info.getAs[Int]("rank")

          list.append(HourCityVideoAccessStat(hour,city,cmsId,times,rank))
        })
        StatDAO.insertHourCityVideoAccessTopN(list)
      })
    } catch {
      case e:Exception =>e.printStackTrace()
    }
  }
}
