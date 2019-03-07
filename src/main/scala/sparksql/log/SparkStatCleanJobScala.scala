package sparksql.log

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkStatCleanJobScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
    val accessRDD = spark.sparkContext.textFile("hdfs://192.168.195.100:9000/mooc/output/part*")

//    accessRDD.take(5).foreach(println(_))
    //RDD=>DF
    val accessDF = spark.createDataFrame(accessRDD.map(x=>AccessConvertUtil.parseLog(x)),AccessConvertUtil.struct)

//    accessDF.show()

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("hour")
        .save("hdfs://192.168.195.100:9000/mooc/clean/")


    spark.stop()


  }

}
