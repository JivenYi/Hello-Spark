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
      .format("com.hollysys.spark.sql.datasource.rest.RestDataSource")
      .option("url", "http://model-opcua-hollysysdigital-test.hiacloud.net.cn/aggquery/query/queryPointHistoryData")
      .option("params", "{\n    \"startTime\": \"1543887720000\",\n    \"endTime\": \"1543891320000\",\n    \"maxSizePerNode\": 1000,\n    \"nodes\": [\n        {\n            \"uri\": \"/SymLink-10000012030100000-device/5c174da007a54e0001035ddd\"\n        }\n    ]\n}")
      .option("xPath", "$.result.historyData")
      //`response` ARRAY<STRUCT<`historyData`:ARRAY<STRUCT<`s`:INT,`t`:LONG,`v`:FLOAT>>>>
      .option("schema", "`s` INT,`t` LONG,`v` DOUBLE")
      .load()
    df.printSchema()
    df.show(false)
  }

}
