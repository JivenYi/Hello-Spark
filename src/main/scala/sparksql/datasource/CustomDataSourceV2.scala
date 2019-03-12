package sparksql.datasource

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

/**
  * 创建DataSource提供类
  * 1.继承DataSourceV2向Spark注册数据源
  * 2.继承ReadSupport支持读数据
  */
class CustomDataSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport{
  /**
    * 创建Reader
    * @param options 用户定义的options
    * @return 自定义的DataSourceReader
    */
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new CustomDataSourceV2Reader(options)
  }

  /**
    * 创建Writer
    *
    * @param writeUUID jobId
    * @param schema schema
    * @param mode 保存模式
    * @param options 用于定义的option
    * @return
    */
  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = ???
}
