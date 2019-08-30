package com.wugui.sparkstarter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 两个简单使用的例子
 * 1. 读取文件打印每行 （读取hdfs文件）
 *          => 地址要和hdfs目录下的core-site.xml一样。
 *          => 参考：![](https://raw.githubusercontent.com/huzekang/picbed/master/20190625000822.png)
 * 2. wordcount（读取本地文件）
 **/
public class SparkStarter {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[5]")
                .setAppName("com.wugui.SparkStarter");
        //之后你用的是Rdd
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // Should be some file on remote hdfs
        JavaRDD<String> stringJavaRDD = sc.textFile("hdfs://cdh01:8020/tmp/spark_starter/app_log.txt");
        stringJavaRDD.foreach(o -> System.out.println(o));

        // Should be some file on your system
        String logFile = "file:///Users/huzekang/study/bigdata-starter/spark-starter/src/main/resources/kv1.txt";

        JavaRDD<String> logData = sc.textFile(logFile).cache();
        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter( s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        sc.stop();

    }
}
