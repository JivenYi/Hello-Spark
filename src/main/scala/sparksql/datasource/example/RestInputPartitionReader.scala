package sparksql.datasource.example

import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

/**
  * RestInputPartitionReader类
  * @param url REST服务的的api
  * @param params 请求需要的参数
  * @param xPath JSON数据的xPath
  */
class RestInputPartitionReader(url: String, params: String, xPath: String) extends InputPartitionReader[InternalRow]{
  // 使用Iterator模拟数据
  val data: Iterator[Seq[AnyRef]] = getIterator

  override def next(): Boolean = {
    data.hasNext
  }

  override def get(): InternalRow = {
    val seq = data.next().map {
      // 浮点类型会自动转为BigDecimal，导致Spark无法转换
      case decimal: BigDecimal =>
        decimal.doubleValue()
      case x => x
    }
    //:_* 代表将seq转换成参数序列
    InternalRow(seq: _*)
  }

  override def close(): Unit = {
    println("close source")
  }

  def getIterator: Iterator[Seq[AnyRef]] = {
    import scala.collection.JavaConverters._
    val res: List[AnyRef] = RestDataSource.requestData(url, params, xPath)
    res.map(r => {
      r.asInstanceOf[JSONObject].asScala.values.toList
    }).toIterator
  }

}
