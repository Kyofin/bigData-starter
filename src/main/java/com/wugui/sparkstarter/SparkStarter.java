package com.wugui.sparkstarter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @program: spark-starter
 * @author: huzekang
 * @create: 2019-06-20 21:28
 **/
public class SparkStarter {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[5]")
                .setAppName("SparkStarter");
        //之后你用的是Rdd
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> stringJavaRDD = sc.textFile("/Users/huzekang/study/spark-starter/src/main/resources/students.txt");

        stringJavaRDD.foreach(o -> System.out.println(o));


    }
}
