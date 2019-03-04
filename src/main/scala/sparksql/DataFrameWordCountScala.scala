package sparksql

import org.apache.spark.sql.SparkSession


object DataFrameWordCountScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
    import spark.implicits._
    val lines = spark.read.textFile("D:\\wordcount.txt")
    val words = lines.flatMap(x=>x.split(","))

    words.show()
    //dataset\dataframe api使用
    import org.apache.spark.sql.functions._
    words.groupBy($"value" as "word").agg(count($"value") as "counts").sort($"counts" desc).show()

    //sql 使用
    words.createOrReplaceTempView("table_wc")
    spark.sql("select value,count(value) as counts from table_wc group by value order by counts desc ").show()

    spark.stop()


  }

}
