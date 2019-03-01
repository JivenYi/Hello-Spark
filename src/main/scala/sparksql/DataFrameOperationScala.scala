package sparksql

import org.apache.spark.sql.SparkSession

object DataFrameOperationScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
    // This import is needed to use the $-notation
    import spark.implicits._
    val peopleDF = spark.read.format("json").load(this.getClass.getResource("/people.json").getPath)
    peopleDF.printSchema()

    peopleDF.show()
    //对于想要age字段加上10，这种方式不行
    peopleDF.select("name","age").show()

    peopleDF.select(peopleDF.col("name"),(peopleDF.col("age")+10).alias("age2")).show()

    peopleDF.select($"name",($"age"+10).alias("age2")).show()

    peopleDF.filter($"age">19).show()

    peopleDF.groupBy($"age").count().show()

    spark.stop()
  }
}
