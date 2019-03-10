package sparksql.datasource.example

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}

/**
  * RestInputPartition 工厂类
  * @param url
  * @param params
  * @param xPath
  */
class RestInputPartition(url: String, params: String, xPath: String) extends InputPartition[InternalRow]{
  override def createPartitionReader(): InputPartitionReader[InternalRow] = ???
}
