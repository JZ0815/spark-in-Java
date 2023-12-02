import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class UDF {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparkSQL-in-Java").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")  // in windows, automatically created when run
                .getOrCreate();
        spark.udf().register("hasPassed", grade -> {return grade.equals("A+")
                || grade.equals("A")
                || grade.equals("B")
                || grade.equals("C");
            }, DataTypes.BooleanType);
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        //dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));
        dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade")));

        dataset.show();


        spark.close();
    }
}
