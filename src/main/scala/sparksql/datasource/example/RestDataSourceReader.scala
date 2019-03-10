package sparksql.datasource.example

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType

/**
  * 创建RestDataSourceReader
  * @param url          REST服务的的api
  * @param params       请求需要的参数
  * @param xPath        JSON数据的xPath
  * @param schemaString 用户传入的schema字符串
  */
class RestDataSourceReader(url: String, params: String, xPath: String, schemaString: String) extends DataSourceReader{

  /**
    * 生成schema
    * 使用StructType.fromDDL方法将schema字符串转成StructType类型
    * @return
    */
  override def readSchema(): StructType = StructType.fromDDL(schemaString)

  /**
    * 创建工厂类
    * @return list[InputPartition]
    */
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    import collection.JavaConverters._
    Seq(
      new RestInputPartition(url,params,xPath).asInstanceOf[InputPartition[InternalRow]]
    ).asJava
  }
}
