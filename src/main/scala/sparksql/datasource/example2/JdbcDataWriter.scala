package sparksql.datasource.example2

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class JdbcDataWriter(url: String,
                     user: String,
                     password: String,
                     table: String,
                     schema: StructType) extends DataWriter[InternalRow] with Logging{
  private var conn: Connection = null
  private var stmt: PreparedStatement = null

  override def write(record: InternalRow): Unit = {
    if (stmt == null) {
      conn = DriverManager.getConnection(url, user, password)
      conn.setAutoCommit(false)

      val columns = schema.fields.map(_.name).mkString(", ")
      val placeholders = ("?" * schema.fields.length).mkString(", ")
      stmt = conn.prepareStatement(s"REPLACE INTO $table ($columns) VALUES ($placeholders)")
    }

    schema.fields.map(_.name).zipWithIndex.foreach { case (name, index) =>
      name match {
        case "id" => stmt.setInt(index + 1, record.getInt(index))
        case "emp_name" => stmt.setString(index + 1, record.getString(index))
        case "dep_name" => stmt.setString(index + 1, record.getString(index))

      }
    }

    logInfo(stmt.toString)
    stmt.executeUpdate()
  }

  override def commit(): WriterCommitMessage = {
    conn.commit()
    conn.close()
    null

  }

  override def abort(): Unit = {
    conn.rollback()
    conn.close()
  }
}
