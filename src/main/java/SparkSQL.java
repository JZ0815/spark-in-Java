import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQL {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparkSQL-in-Java").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")  // in windows, automatically created when run
                .getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        dataset.show();
        long count = dataset.count();
        System.out.println("There are " + count + " records.");
        spark.close();
    }
}
