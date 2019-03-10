package sparksql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class DataFrameRDDJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[2]").appName("DataFrameRDDJava").getOrCreate();
        String fileUrl = DataFrameRDDJava.class.getResource("/infos.txt").getPath();
        JavaRDD<String> StirngRDD = spark.read().textFile(fileUrl).javaRDD();
        JavaRDD<Info> infoRDD = StirngRDD.map(x -> new Info(Integer.parseInt(x.split(",")[0]), x.split(",")[1], Integer.parseInt(x.split(",")[2])));
        Dataset<Row> infoDF = spark.createDataFrame(infoRDD, Info.class);

        infoDF.show();


        spark.stop();
    }
}
