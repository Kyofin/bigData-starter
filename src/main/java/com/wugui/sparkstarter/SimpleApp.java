package com.wugui.sparkstarter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
    public static void main(String[] args) {
        // Should be some file on your system
        String logFile = "file:///Users/huzekang/study/spark-starter/src/main/resources/students.txt";
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 减少日志输出
        sc.setLogLevel("ERROR");


        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();

        long numBs = logData.filter( s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        sc.stop();
    }
}
