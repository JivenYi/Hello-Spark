package sparksql.datasource.example2

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

class JdbcInputPartitionReader(url: String,
                               user: String,
                               password: String,
                               table: String,
                               columns: Seq[String],
                               wheres: Seq[String],
                               partition: String) extends InputPartitionReader[InternalRow] with Logging{


  private var conn: Connection = null
  private var rs: ResultSet = null

  override def next(): Boolean = {
    if (rs == null) {
      conn = DriverManager.getConnection(url, user, password)

      val sqlBuilder = new StringBuilder()
      sqlBuilder ++= s"SELECT ${columns.mkString(", ")} FROM $table WHERE $partition"
      if (wheres.nonEmpty) {
        sqlBuilder ++= " AND " + wheres.mkString(" AND ")
      }
      val sql = sqlBuilder.toString
      logInfo(sql)


      val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)
      stmt.setFetchSize(1000)
      rs = stmt.executeQuery()
    }

    rs.next()
  }

  override def get(): InternalRow = {
    val values = columns.map {
      case "hour" => UTF8String.fromString(rs.getString("hour"))
      case "cms_id" => rs.getInt("cms_id")
      case "times" => rs.getInt("times")

    }

    InternalRow.fromSeq(values)
  }

  override def close(): Unit = {
    conn.close()
  }
}
