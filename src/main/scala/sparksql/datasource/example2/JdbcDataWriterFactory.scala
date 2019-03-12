package sparksql.datasource.example2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class JdbcDataWriterFactory(url: String,
                            user: String,
                            password: String,
                            table: String,
                            schema: StructType) extends DataWriterFactory[InternalRow]{
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new JdbcDataWriter(url,user,password,table,schema)
  }
}
