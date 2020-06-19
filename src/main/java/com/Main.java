package com;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);

        JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));
        sqrtRdd.collect().forEach(System.out::println);

        System.out.println(result);

        // how many elements in sqrtRdd
        System.out.println(sqrtRdd.count());

        // using map and reduce for counting
        JavaRDD<Integer> singleIntegerRdd = sqrtRdd.map(value -> 1);
        Integer count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);

        System.out.println("count: " + count);

        sc.close();
    }
}
