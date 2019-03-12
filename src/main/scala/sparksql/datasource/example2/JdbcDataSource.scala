package sparksql.datasource.example2

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

class JdbcDataSource extends DataSourceV2 with ReadSupport with WriteSupport{
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new JdbcDataSourceReader(options.get("url").get(),
                             options.get("user").get(),
                             options.get("password").get(),
                             options.get("table").get(),
                             options.get("schema").get())
  }

  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new JdbcDataSourceWriter(
      options.get("url").get(),
      options.get("user").get(),
      options.get("password").get(),
      options.get("table").get(),
      schema
    ))
  }
}
