package sparksql.datasource.example2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object JdbcTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName(this.getClass.getSimpleName).getOrCreate()
    val data = spark.read.format("sparksql.datasource.example2.JdbcDataSource")
      .option("url", "jdbc:mysql://192.168.195.100:3306/imooc")
      .option("user", "root")
      .option("password", "123456")
      .option("table", "hour_video_access_topn_stat")
      .option("schema", "`hour` STRING,`cms_id` INT,`times` INT").load()

    import spark.implicits._


    val df = data.select($"hour", $"times").where($"times" === 3)
    df.show()
    df.explain(true)
    spark.stop()
  }

}
