package sparksql;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Array;
import scala.Function1;
import scala.collection.TraversableOnce;

import java.util.Arrays;
import java.util.Iterator;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class DataFrameWordCountJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[2]").appName("DataFrameWordCountJava").getOrCreate();
        Dataset<String> lines = spark.read().textFile("D:\\wordcount.txt");
//        Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                return Arrays.asList(s.split(",")).iterator();
//            }
//        }, Encoders.STRING());
        Dataset<String> words = lines.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(",")).iterator(), Encoders.STRING());

        //words.show();
        //dataframe\dataset api
        words.groupBy(col("value")).agg(count("value").alias("counts")).sort(col("counts").desc()).show();

        //sql
        words.createOrReplaceTempView("table_wc");
        spark.sql("select value,count(value) as counts from table_wc group by value order by counts desc ").show();
        spark.stop();
    }
}
