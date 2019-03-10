package sparksql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameRDDScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
    //    inferReflection(spark)
    program(spark)
    spark.stop()
  }

  def program(spark: SparkSession): Unit = {
    import spark.implicits._
    val infoRDD = spark.sparkContext.textFile(this.getClass.getResource("/infos.txt").getPath)
    val rowRDD = infoRDD.map(_.split(",")).map(x => Row(x(0).toInt, x(1), x(2).toInt))
    val structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))
    val infoDF = spark.createDataFrame(rowRDD,structType)
    infoDF.show()
    infoDF.filter($"age" > 30).show()

    infoDF.createOrReplaceTempView("info")
    spark.sql("select * from info where age >30").show()
  }

  def inferReflection(spark: SparkSession) = {
    val infoRDD = spark.sparkContext.textFile(this.getClass.getResource("/infos.txt").getPath)
    import spark.implicits._
    val infoDF = infoRDD.map(_.split(",")).map(x => Info(x(0).toInt, x(1), x(2).toInt)).toDF()
    infoDF.show()

    infoDF.filter($"age" > 30).show()

    infoDF.createOrReplaceTempView("info")
    spark.sql("select * from info where age >30").show()
  }

  case class Info(id: Int, name: String, age: Int)

}
