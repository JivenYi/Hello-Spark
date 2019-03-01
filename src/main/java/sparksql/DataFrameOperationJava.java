package sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;
public class DataFrameOperationJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[2]").appName("DataFrameOperationJava").getOrCreate();
        String fileUrl = DataFrameOperationJava.class.getResource("/people.json").getPath();
        Dataset<Row> peopleDF = spark.read().format("json").load(fileUrl);

        peopleDF.printSchema();

        peopleDF.show();

        peopleDF.select(col("name"),col("age").plus(10)).show();

        peopleDF.select(col("name"),col("age").plus(10).alias("age2")).show();

        peopleDF.filter(col("age").gt(19)).show();

        peopleDF.groupBy(col("age")).count().show();

        spark.stop();

    }
}
