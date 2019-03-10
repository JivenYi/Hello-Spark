package sparksql.datasource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}

/**
  * 自定义InputPartition类
  */
class CustomDataSourceV2InputPartition extends InputPartition[InternalRow] {
  /**
    * 重写createPartitionReader方法，用来实例化自定义的InputPartitionReader
    * @return 自定义的InputPartitionReader
    */
  override def createPartitionReader(): InputPartitionReader[InternalRow] = new CustomPartitionReader
}
