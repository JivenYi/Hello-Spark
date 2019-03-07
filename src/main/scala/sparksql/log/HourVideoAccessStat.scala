package sparksql.log

/**
  * 每小时访问视频统计实体类
  * @param hour
  * @param cmdId
  * @param times
  */
case class HourVideoAccessStat(hour:String,cmdId:Long,times:Long)
