package sparksql.datasource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

/**
  * 自定义InputPartitionReader类
  */
class CustomPartitionReader extends InputPartitionReader[InternalRow]{
  /**
    * 是否有下一条数据
    * @return boolean
    */
  override def next(): Boolean = ???

  /**
    * 获取数据
    * 当next为true时会调用get方法获取数据
    * @return
    */
  override def get(): InternalRow = ???

  /**
    * 关闭资源
    */
  override def close(): Unit = ???
}
