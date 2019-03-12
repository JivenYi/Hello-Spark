package sparksql.datasource.example

import org.apache.spark.sql.SparkSession

object RestDataSourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val df = spark.read
      .format("sparksql.datasource.example.RestDataSource")
      .option("url", "http://localhost:8080/test")
      .option("params", "{\"name\":\"yi\",\"age\":2}")
      .option("xPath", "/")
      .option("schema", "`name` STRING,`age` INT")
      .load()
    df.printSchema()
    df.show(false)
  }

}
