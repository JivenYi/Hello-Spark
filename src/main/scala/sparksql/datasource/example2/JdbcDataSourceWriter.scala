package sparksql.datasource.example2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class JdbcDataSourceWriter(url: String,
                           user: String,
                           password: String,
                           table: String,
                           schema: StructType) extends DataSourceWriter{
  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new JdbcDataWriterFactory(url,user,password,table,schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = ???

  override def abort(messages: Array[WriterCommitMessage]): Unit = ???
}
