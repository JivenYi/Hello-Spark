package sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSessionJava {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("SparkSessionJava").getOrCreate();
        String fileUrl = SparkSessionJava.class.getResource("/people.json").getPath();
        Dataset<Row> ds = sparkSession.read().json(fileUrl);
        ds.show();
        sparkSession.stop();
    }
}
