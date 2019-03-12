package sparksql.datasource.example2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}

class JdbcInputPartition(url: String,
                         user: String,
                         password: String,
                         table: String,
                         columns:Array[String],
                         wheres:Array[String],
                         partition:String) extends InputPartition[InternalRow]{
  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new JdbcInputPartitionReader(url,user,password,table,columns,wheres,partition)
  }
}
