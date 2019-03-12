package sparksql.datasource

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
  * 自定义的DataSourceReader
  * 继承DataSourceReader
  * 重写readSchema方法用来生成schema
  * 重写planInputPartitions,返回InputPartitions集合
  * @param options
  */
class CustomDataSourceV2Reader(options: DataSourceOptions) extends DataSourceReader with DataSourceWriter{
  /**
    * 生成schema信息
    * @return
    */
  override def readSchema(): StructType = ???

  /**
    * 每一个InputPartition负责为一个RDD partition创建一个data reader(多个就可以用来并发获取数据？)
    * @return
    */
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    import collection.JavaConverters._
    Seq(
     new CustomDataSourceV2InputPartition().asInstanceOf[InputPartition[InternalRow]]
    ).asJava
  }

  /**
    * 创建DataWriter
    * @return
    */
  override def createWriterFactory(): DataWriterFactory[InternalRow] = ???

  /**
    * 每个分区触发一次
    * @param messages
    */
  override def commit(messages: Array[WriterCommitMessage]): Unit = ???

  /**
    * 回滚：当write发生异常时触发该方法
    * @param messages
    */
  override def abort(messages: Array[WriterCommitMessage]): Unit = ???
}
