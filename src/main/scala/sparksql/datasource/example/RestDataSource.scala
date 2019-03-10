package sparksql.datasource.example

import com.alibaba.fastjson.{JSONArray, JSONPath}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.http.client.fluent.Request
import org.apache.http.entity.ContentType
/**
  * 这里主要是从REST接口里获取JSON格式的数据，然后生成DataFrame数据源
  */
class RestDataSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new RestDataSourceReader(
      options.get("url").get(),
      options.get("params").get(),
      options.get("xPath").get(),
      options.get("schema").get()
    )
  }
}

object RestDataSource {
  def requestData(url: String, params: String, xPath: String): List[AnyRef] = {
    import scala.collection.JavaConverters._
    val response = Request.Post(url).bodyString(params, ContentType.APPLICATION_JSON).execute()
    JSONPath.read(response.returnContent().asString(), xPath)
      .asInstanceOf[JSONArray].asScala.toList
  }
}

