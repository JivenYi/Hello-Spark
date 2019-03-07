package sparksql.log

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Mysql 操作工具类
  */
object MysqlUtils {

  /**
    * 获取数据库连接
    */
  def getConnection(): Connection ={
    DriverManager.getConnection("jdbc:mysql://192.168.195.100:3306/imooc?user=root&password=123456&autoReconnect=true&useUnicode=true&createDatabaseIfNotExist=true&characterEncoding=utf8&useSSL=false&serverTimezone=UTC")
  }

  /**
    * 释放数据库链接等资源
    * @param connection
    * @param pstmt
    */
  def release(connection: Connection,pstmt:PreparedStatement): Unit ={
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e:Exception =>e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}
