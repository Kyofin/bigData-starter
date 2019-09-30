package com.wugui.sparkstarter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @program: bigdata-starter
 * @author: huzekang
 * @create: 2019-09-29 15:30
 **/
public class ClickHouseTest {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("clickhouse")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read()
                .format("jdbc")
                .option("user", "yibo_test")
                .option("password", "yibo123")
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("url", "jdbc:oracle:thin:@192.168.1.130:1521:orcl")
                .option("dbtable", "TB_LIS_INDICATORS")
                .load();


        Properties connectionProperties = new Properties();
        connectionProperties.put("driver", "ru.yandex.clickhouse.ClickHouseDriver");
        connectionProperties.put("batchsize", "50000");
        // 设置事务
        connectionProperties.put("isolationLevel", "NONE");
        // 设置并发
        connectionProperties.put("numPartitions", "5");

        df.write()
                .mode(SaveMode.Append)
                .jdbc("jdbc:clickhouse://192.168.1.39:8123", "TB_LIS_INDICATORS", connectionProperties);
    }
}
