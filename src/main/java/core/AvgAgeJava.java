package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class AvgAgeJava {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("AvgAgeJava");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //(id,age)
        List<String> data = Arrays.asList("1,12", "2,21", "3,45", "4,60", "5,34");
        JavaRDD<String> ageData = jsc.parallelize(data);
        //(id,age) => (age,1)
        JavaPairRDD<Integer, Integer> age = ageData.mapToPair(x -> new Tuple2<Integer, Integer>(Integer.parseInt(x.split(",")[1]), 1));
        Tuple2<Integer, Integer> result = age.reduce((x1, x2) -> new Tuple2<>(x1._1 + x2._1, x1._2 + x2._2));
        int avgAge = result._1 / result._2;
        System.out.println(avgAge);

        jsc.stop();
    }
}
