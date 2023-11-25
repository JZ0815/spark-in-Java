import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RddJoins {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("spark-in-Java").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        // (userid, visited)
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        // (userid, name)
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Mary"));
        usersRaw.add(new Tuple2<>(6, "Alice"));


        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        // JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);
        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visits.leftOuterJoin(users);
        //joinedRdd.collect().forEach(System.out::println);
        joinedRdd.collect().forEach(it -> System.out.println((it._2._2.get().toLowerCase())));

        sc.close();
    }
}
