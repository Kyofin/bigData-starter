package com.wugui.sparkstarter.hbase;

import org.apache.spark.sql.*;

/**
 * @program: bigdata-starter
 * @author: huzekang
 * @create: 2019-08-30 18:07
 **/
public class SparkHbasePhoenix {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder().appName("SparkHBaseDataFrame").master("local").getOrCreate();

        Dataset<Row> dataset = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "patient_test")
                .load();
        dataset.printSchema();
        dataset.select("ID").show();
        dataset.createTempView("abc");
        sparkSession.sql("select empi,id from abc  where id> 1005  ").show();
    }
}
