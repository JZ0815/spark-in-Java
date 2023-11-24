import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordsCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark-in-Java").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/sparkDev.txt");

        sc.close();
    }
}