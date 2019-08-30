package com.wugui.sparkstarter.es;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

/**
 * spark 结合elasticsearch 例子
 * 参考资料：https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html#spark-native
 */
public class EsSparkTest {

    public static void main(String[] args) {
//        new EsSparkTest().writeEs();
//        new EsSparkTest().readEs();
        new EsSparkTest().writeBeanEs();
    }

    /**
     * 以map方式存入es
     */
    public void writeEs() {
        String elasticIndex = "spark/docs";
        SparkConf sparkConf = new SparkConf()
                .setAppName("writeEs")
                .setMaster("local[*]")
                .set("es.index.auto.create", "true")
                .set("es.nodes", "192.168.1.25")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter
        Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
        Map<String, ?> airports = ImmutableMap.of("city", "广州", "airportName", "广州白云机场");
        JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
        JavaEsSpark.saveToEs(javaRDD, elasticIndex);
    }

    /**
     * 以对象存入es
     */
    public void writeBeanEs() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("writeEs")
                .setMaster("local[*]")
                .set("es.index.auto.create", "true")
                .set("es.nodes", "192.168.1.25")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter


        TripBean upcoming = new TripBean("广州白云机场", "昆明机场");
        TripBean lastWeek = new TripBean("昆明机场", "广州白云机场");

        JavaRDD<TripBean> javaRDD = jsc.parallelize(
                ImmutableList.of(upcoming, lastWeek));
        JavaEsSpark.saveToEs(javaRDD, "spark/docs");
    }

    public void readEs() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("writeEs")
                .setMaster("local[*]")
                .set("es.index.auto.create", "true")
                .set("es.nodes", "192.168.1.25")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter
        JavaRDD<Map<String, Object>> searchRdd = JavaEsSpark.esRDD(jsc, "spark/docs", "?q=广州").values();
        for (Map<String, Object> item : searchRdd.collect()) {
            item.forEach((key, value) -> System.out.println("search key:" + key + ", search value:" + value));
        }
        sparkSession.stop();
    }



}