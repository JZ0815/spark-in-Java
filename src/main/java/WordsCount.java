import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordsCount {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        SparkConf conf = new SparkConf().setAppName("spark-in-Java").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lineRdd = sc.textFile("src/main/resources/sparkDev.txt");
        lineRdd.flatMap(k -> Arrays.asList(k.split(" ")).iterator())
                .mapToPair(k -> new Tuple2<>(k, 1))
                .reduceByKey((k1, k2) -> k1 + k2)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2));

        sc.close();
    }
}