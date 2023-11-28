import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

public class SparkSQL {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparkSQL-in-Java").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")  // in windows, automatically created when run
                .getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        dataset.show();
        dataset.persist(StorageLevel.MEMORY_AND_DISK());
        long count = dataset.count();
        System.out.println("There are " + count + " records.");
        Row firstRow = dataset.first();
        String subject = firstRow.get(2).toString();
        System.out.println(subject);

        int yr = Integer.parseInt(firstRow.getAs("year"));
        System.out.println(yr);

        Dataset<Row> filteredResult1 = dataset.filter("subject = 'Math'");
        filteredResult1.show();

        Dataset<Row> filteredResult2 = dataset.filter((FilterFunction<Row>) row ->
                Integer.parseInt(row.getAs("year")) >= 2007) ;
        filteredResult2.show();

        Column subjectColumn = dataset.col("subject");
        Column yearColumn = dataset.col("year");
        Dataset<Row> filteredResult3 = dataset.filter(subjectColumn.equalTo("History")
                .and(yearColumn.geq(2007))) ;
        filteredResult3.show();


        spark.close();
    }
}
