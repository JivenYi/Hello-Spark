package sparksql.log

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换(输入=》输出)工具类
  */
object AccessConvertUtil {
  val struct = StructType(
    Array(
      StructField("url", StringType, nullable = true),
      StructField("cmsType", StringType, nullable = true),
      StructField("cmsId", LongType, nullable = true),
      StructField("traffic", LongType, nullable = true),
      StructField("ip", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("time", StringType, nullable = true),
      StructField("hour", StringType, nullable = true)
    )
  )

  /**
    * 根据输入的每一行信息转换成输出的样式
    * @param log 输入的每一行记录信息
    */
  def parseLog(log:String) ={
    try {
      val splits = log.split("\t")

      val url = splits(1)
      val traffic  = splits(2).toLong
      val ip = splits(3)

      val domain = "http://www.imooc.com/"
      val cms = url.substring(url.indexOf(domain)+domain.length)
      val cmsTypeId = cms.split("/")
      var cmsId = 0L
      var cmsType = ""

      if(cmsTypeId.length>1){
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }
      val city = IpUtils.getCity(ip)
      val time = splits(0)
      val hour = time.substring(0,13).replaceAll("-","").replaceAll(" ","")
      Row(url,cmsType,cmsId,traffic,ip,city,time,hour)
    }catch {
      case ex:Exception=>Row("","",0L,0L,"","","","")
    }

  }
}
