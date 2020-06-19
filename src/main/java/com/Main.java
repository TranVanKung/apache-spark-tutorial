package com;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 7 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("StaringSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // reduceByKey
//        sc.parallelize(inputData)
//                .mapToPair(
//                        rawValue -> new Tuple2<>(rawValue.split("")[0], 1L)
//                )
//                .reduceByKey((value1, value2) -> value1 + value2)
//                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        // groupBykey
        sc.parallelize(inputData)
                .mapToPair(
                        rawValue -> new Tuple2<>(rawValue.split("")[0], 1L)
                )
                .groupByKey()
                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));


        sc.close();
    }
}
