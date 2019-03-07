package sparksql.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * 各个维度的统计DAO操作
  */
object StatDAO {

  /**
    * 批量保存到数据库
    *
    * @param list
    */
  def insertHourVideoAccessTopN(list: ListBuffer[HourVideoAccessStat]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MysqlUtils.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into hour_video_access_topn_stat(hour,cms_id,times) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for (element <- list){
        pstmt.setString(1,element.hour)
        pstmt.setLong(2,element.cmdId)
        pstmt.setLong(3,element.times)
        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }
  def insertHourCityVideoAccessTopN(list: ListBuffer[HourCityVideoAccessStat]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MysqlUtils.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into hour_city_viideo_access_topn_stat(hour,city,cms_id,times,times_rank) values (?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for (element <- list){
        pstmt.setString(1,element.hour)
        pstmt.setString(2,element.city)
        pstmt.setLong(3,element.cmdId)
        pstmt.setLong(4,element.times)
        pstmt.setInt(5,element.timesRank)
        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }
}
