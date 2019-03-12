package sparksql.datasource.example2

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

class JdbcDataSourceReader(url: String,
                           user: String,
                           password: String,
                           table: String,
                           schema: String) extends DataSourceReader with SupportsPushDownRequiredColumns with SupportsPushDownFilters {

  override def readSchema(): StructType = requiredSchema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    import collection.JavaConverters._
    val columns = readSchema().fields.map(_.name)
    Seq((1, 12), (13, 32)).map{
      case (minId, maxId) =>
        val partition = s"times BETWEEN $minId AND $maxId"
        new JdbcInputPartition(url,user,password,table,columns,wheres,partition).asInstanceOf[InputPartition[InternalRow]]
    }.asJava
  }

  var filters = Array.empty[Filter]
  var wheres = Array.empty[String]

  var requiredSchema = StructType.fromDDL(schema)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  /**
    * Pushes down filters, and returns filters that need to be evaluated after scanning.
    * @param filters
    * @return filters that need to be evaluated after scanning
    */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val supported = ListBuffer.empty[Filter]
    val unsupported = ListBuffer.empty[Filter]
    val wheres = ListBuffer.empty[String]

    //只有等于可以被下推
    filters.foreach {
      case filter: EqualTo => {
        supported += filter
        wheres += s"${filter.attribute} = '${filter.value}'"
      }
      case filter => unsupported += filter
    }

    this.filters = supported.toArray
    this.wheres = wheres.toArray

    unsupported.toArray
  }

  override def pushedFilters(): Array[Filter] = filters
}
