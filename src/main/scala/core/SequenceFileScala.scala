package core

import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}

object SequenceFileScala {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)
    val personRdd = sc.parallelize(Array(PersonScala("yjw",25),PersonScala("ryf",25)))
    // 以NullWritable 为key,构建kv结构.SequenceFile需要kv结构才能存储,NullWritable不占存储
    //压缩参数可选用 example:Option(classOf[GzipCodec])
    personRdd.map((NullWritable.get(),_))
      .coalesce(1).saveAsSequenceFile(args(0))

    val person = sc.sequenceFile(args(0)+"/part*",classOf[NullWritable],classOf[PersonScala])
      .map{
        case(_,y)=>PersonScala(y.name,y.age)
      }.cache()

    person.foreach(println(_))

    sc.stop()

  }
}
