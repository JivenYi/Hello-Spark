package sparksql.log


import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间解析工具类
  * SimpleDataFormat 线程不安全的，不建议用。建议用org.apache.commons.lang3.time.FastDateFormat
  */
object DateUtilsScala {
  //输入文件日期格式
  //[10/Nov/2016:00:01:02 +0800]
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  //输出文件日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 获取时间 yyyy-MM-dd HH:mm:ss
    */
  def parse(time: String): String = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 获取输入日志时间：Long类型
    *
    * @param time
    * @return
    */
  def getTime(time: String): Long = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0L
      }
    }
  }
}
