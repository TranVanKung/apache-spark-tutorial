package com;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(22);
        inputData.add(32);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("StaringSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map(
                value -> new Tuple2<>(value, Math.sqrt(value))
        );
        Tuple2<Integer, Double> myValue = new Tuple2(9, 3.0);

        sc.close();
    }
}
