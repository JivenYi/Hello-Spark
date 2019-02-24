package wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


import java.util.Arrays;
import java.util.List;

public class WordCountJava {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountJava");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        List<String> data = Arrays.asList("spark,kafka,mysql", "spark,hadoop,flink", "spark,hadoop,hive");
        JavaPairRDD<String, Integer> wordCount = jsc.parallelize(data).flatMap(x -> Arrays.asList(x.split(",")).iterator())
                .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                .reduceByKey((x, y) -> x + y);
        wordCount.foreach(x-> System.out.println(x));
        jsc.stop();

    }
}
