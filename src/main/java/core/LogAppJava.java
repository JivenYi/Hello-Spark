package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class LogAppJava {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("LogAppJava");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        String fileUrl = LogAppJava.class.getResource("/access.log").getPath();
        JavaRDD<String> logInfoRdd = jsc.textFile(fileUrl);
        JavaPairRDD<String, LogInfoJava> logInfoJavaRdd = logInfoRdd.mapToPair(x -> {
            long timestamp = Long.parseLong(x.split("\t")[0]);
            String deviceId = x.split("\t")[1];
            long upTraffic = Long.parseLong(x.split("\t")[2]);
            long downTraffic = Long.parseLong(x.split("\t")[3]);
            return new Tuple2<String, LogInfoJava>(x.split("\t")[1], new LogInfoJava(timestamp, upTraffic, downTraffic));
        });
        JavaPairRDD<String, LogInfoJava> reduceRdd = logInfoJavaRdd.reduceByKey((x1, x2) -> {
            long timestamp = x1.getTimestamp() > x2.getTimestamp() ? x2.getTimestamp() : x1.getTimestamp();
            long upTraffic = x1.getUpTraffic() + x2.getUpTraffic();
            long downTraffic = x1.getDownTraffic() + x2.getDownTraffic();
            return new LogInfoJava(timestamp, upTraffic, downTraffic);
        });
        JavaPairRDD<LogInfoJava, String> mapRdd = reduceRdd.mapToPair(x -> new Tuple2<LogInfoJava, String>(x._2, x._1));
        List<Tuple2<String, LogInfoJava>> result = mapRdd.sortByKey(false).mapToPair(x -> new Tuple2<String, LogInfoJava>(x._2, x._1)).take(5);
        result.forEach(x-> System.out.println(x));

        jsc.stop();
    }
}
