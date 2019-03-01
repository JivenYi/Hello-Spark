package sparksql

import org.apache.spark.sql.SparkSession

object SparkSessionScala {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
    val fileUrl = this.getClass.getResource("/people.json").getPath
    val dataFrame = sparkSession.read.json(fileUrl)
    dataFrame.show()
    sparkSession.stop()
  }

}
