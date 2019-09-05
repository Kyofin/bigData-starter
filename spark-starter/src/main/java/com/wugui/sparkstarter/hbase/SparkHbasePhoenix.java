package com.wugui.sparkstarter.hbase;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.phoenix.spark.*;


/**
 * @program: bigdata-starter
 * @author: huzekang
 * @create: 2019-08-30 18:07
 **/
public class SparkHbasePhoenix {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder().appName("SparkHBaseDataFrame").master("local").getOrCreate();

        // 使用phoenix jdbc连接驱动读取数据
        Dataset<Row> dataset = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "patient_test")
                .load();
        dataset.show();

        // 使用spark sql 的函数替换phone列的所有匹配规则的值
        Dataset<Row> rowDataset = dataset.withColumn("PHONE", regexp_replace(col("PHONE"), "123", "sss"));

        // 使用phoenix spark plugin 写入数据
        rowDataset
                .write()
                .format("org.apache.phoenix.spark")
                .mode(SaveMode.Overwrite)
                .option("table", "patient_test")
                .option("zkUrl", "cdh01:2181")
                .save();


    }
}
