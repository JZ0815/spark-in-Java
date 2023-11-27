import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Scanner;


public class WordsCount {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("spark-in-Java").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*SparkSession sparkSession = SparkSession.builder().appName("TestingSparkSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/Apache/Spark/tmp")
                .getOrCreate();

        SparkSession sparkSession = SparkSession.builder().appName("TestingSparkSql")
                .master("spark://192.168.19.1:7077")
                .config("spark.sql.warehouse.dir", "file:///c:/Apache/Spark/tmp")
                .getOrCreate();*/
        JavaRDD<String> lineRdd = sc.textFile("src/main/resources/sparkDev.txt");
        lineRdd.flatMap(k -> Arrays.asList(k.split(" ")).iterator())
                .filter(wd -> !wd.equals(""))
                .map(wd -> wd.replace("[^a-zA-Z\\s]",""))
                //!filter before reduceByKey to improve performance, narrow transformation
                .mapToPair(k -> new Tuple2<>(k, 1L))
                .reduceByKey((k1, k2) -> k1 + k2)  //there is a shufulling, wide transformation here
                .mapToPair(Tuple2::swap)
                //.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)  //decending order
                .take(10)
                .forEach(k -> System.out.println("\"" + k._2 + "\""+ " has " + k._1));

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        sc.close();
    }
}