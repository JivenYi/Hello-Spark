package core;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SequenceFileJava {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SequenceFileJava").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[]{NullWritable.class});
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        // 以NullWritable 为key,构建kv结构.SequenceFile需要kv结构才能存储,NullWritable不占存储
        //压缩参数可选用 example:Option(classOf[GzipCodec])
        //需要加上set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")配置，默认NullWritable是没有实现序列化的
        JavaPairRDD<NullWritable, PersonJava> data = jsc.parallelizePairs(Arrays.asList(new Tuple2<NullWritable, PersonJava>(NullWritable.get(), new PersonJava("yjw", 18)),
                new Tuple2<NullWritable, PersonJava>(NullWritable.get(), new PersonJava("ryf", 16))));
        data.coalesce(1).saveAsHadoopFile(args[0],NullWritable.class,PersonJava.class,SequenceFileOutputFormat.class);

        JavaPairRDD<NullWritable, PersonJava> person = jsc.sequenceFile(args[0]+"/part*", NullWritable.class, PersonJava.class);
        person.foreach(x-> System.out.println(x._2));

        jsc.stop();
    }
}
