package sparksql.datasource

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

/**
  * 创建DataSource提供类
  * 1.继承DataSourceV2向Spark注册数据源
  * 2.继承ReadSupport支持读数据
  */
class CustomDataSourceV2 extends DataSourceV2 with ReadSupport{
  /**
    * 创建Reader
    * @param options 用户定义的options
    * @return 自定义的DataSourceReader
    */
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new CustomDataSourceV2Reader(options)
  }
}
