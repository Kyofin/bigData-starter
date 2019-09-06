package com.wugui.sparkstarter.ml;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @program: bigdata-starter
 * @author: huzekang
 * @create: 2019-09-05 20:38
 **/
public class SparkDouban {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        RDD<Row> rdd = sparkSession.read().text("/Users/huzekang/study/bigdata-starter/spark-starter/src/main/resources/data/hot_movies.csv").rdd();
    }
}
