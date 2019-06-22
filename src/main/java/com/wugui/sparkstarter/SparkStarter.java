package com.wugui.sparkstarter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 两个简单使用的例子
 * 1. 读取文件打印每行
 * 2. wordcount
 **/
public class SparkStarter {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[5]")
                .setAppName("SparkStarter");
        //之后你用的是Rdd
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sc.textFile("/Users/huzekang/study/spark-starter/src/main/resources/students.txt");
        stringJavaRDD.foreach(o -> System.out.println(o));

        // Should be some file on your system
        String logFile = "file:///Users/huzekang/study/spark-starter/src/main/resources/kv1.txt";

        JavaRDD<String> logData = sc.textFile(logFile).cache();
        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter( s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        sc.stop();

    }
}
