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
        SparkConf sparkConf = new SparkConf()
                .setAppName("writeEs")
                .setMaster("local[*]")
                .set("es.index.auto.create", "true")
                .set("es.nodes", "192.168.1.25")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

//        new EsSparkTest().writeEs(sparkSession);
//        new EsSparkTest().writeBeanEs(sparkSession);
//        new EsSparkTest().writeJsonEs(sparkSession);

//        new EsSparkTest().readEs(sparkSession);
        new EsSparkTest().readEs2(sparkSession);
    }

    /**
     * 以map方式存入es
     */
    public void writeEs(SparkSession sparkSession) {
        String elasticIndex = "spark/docs";

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter
        Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
        Map<String, ?> airports = ImmutableMap.of("city", "广州", "airportName", "广州白云机场");
        JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
        JavaEsSpark.saveToEs(javaRDD, elasticIndex);
    }

    /**
     * 以对象存入es
     */
    public void writeBeanEs(SparkSession sparkSession) {

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter


        TripBean upcoming = new TripBean("广州白云机场", "昆明机场");
        TripBean lastWeek = new TripBean("昆明机场", "广州白云机场");

        JavaRDD<TripBean> javaRDD = jsc.parallelize(
                ImmutableList.of(upcoming, lastWeek));
        JavaEsSpark.saveToEs(javaRDD, "spark/docs");
    }

    /**
     * 以json方式存入es
     * @param sparkSession
     */
    public void writeJsonEs(SparkSession sparkSession) {
        String json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";
        String json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}";
        String json3 = "{" +
                "    \"flavor\": {" +
                "        \"name\": \"IMS_CMREPO\"," +
                "        \"links\": [" +
                "            {" +
                "                \"href\": \"http://192.168.49.25:8774/v2/29ec86a6f17942f49fdc0bcc0748087b/flavors/00061ec1-4405-40fe-87a5-d06191f6826d\"," +
                "                \"rel\": \"self\"" +
                "            }," +
                "            {" +
                "                \"href\": \"http://192.168.49.25:8774/29ec86a6f17942f49fdc0bcc0748087b/flavors/00061ec1-4405-40fe-87a5-d06191f6826d\"," +
                "                \"rel\": \"bookmark\"" +
                "            }" +
                "        ]," +
                "        \"ram\": 16384," +
                "        \"OS-FLV-DISABLED:disabled\": false," +
                "        \"vcpus\": 8," +
                "        \"swap\": \"\"," +
                "        \"os-flavor-access:is_public\": true," +
                "        \"rxtx_factor\": 1," +
                "        \"OS-FLV-EXT-DATA:ephemeral\": 0," +
                "        \"disk\": 38," +
                "        \"id\": \"00061ec1-4405-40fe-87a5-d06191f6826d\"" +
                "    }" +
                '}';

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> stringRDD = jsc.parallelize(ImmutableList.of(json1, json2,json3));
        JavaEsSpark.saveJsonToEs(stringRDD, "spark/json-trips");
    }



    public void readEs(SparkSession sparkSession) {

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter
        JavaRDD<Map<String, Object>> searchRdd = JavaEsSpark.esRDD(jsc, "spark/docs", "?q=广州").values();
        for (Map<String, Object> item : searchRdd.collect()) {
            item.forEach((key, value) -> System.out.println("search key:" + key + ", search value:" + value));
        }
        sparkSession.stop();
    }


    public void readEs2(SparkSession sparkSession) {
        sparkSession.sqlContext().read().format("es").load("spark/docs").show();
    }


}