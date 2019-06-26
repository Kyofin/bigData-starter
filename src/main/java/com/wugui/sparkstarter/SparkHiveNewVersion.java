package com.wugui.sparkstarter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * spark2.0支持hive的使用方法 (推荐使用这种方式访问hive)
 * 使用SparkSession进行统一上下文
 **/
public class SparkHiveNewVersion {


    public static void main(String[] args) {
        //  定义上下文
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .enableHiveSupport()
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        spark.sql("USE default");
        spark.sql("show tables").show();
        //  在hive中创建student_infos表
        spark.sql("DROP TABLE IF EXISTS student_infos");
        spark.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING,age INT) row format delimited fields terminated by ',' ");
        //  加载本地文件数据到hive表中
        spark.sql("load data local inpath '/Users/huzekang/study/spark-starter/src/main/resources/student_infos.txt' into table student_infos");

        //  在hive中创建student_scores表
        spark.sql("DROP TABLE IF EXISTS student_scores");
        spark.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT) row format delimited fields terminated by ','");
        //  加载本地文件数据到hive表中
        spark.sql("LOAD DATA "
                + "LOCAL INPATH '/Users/huzekang/study/spark-starter/src/main/resources/student_scores.txt'"
                + "INTO TABLE student_scores");

        spark.sql("select * from student_infos").show();
        spark.sql("select * from student_scores").show();
        /**
         * 查询两个hive表合并后,过滤出生成DataFrame
         */
        Dataset goodStudentsDF = spark.sql("SELECT si.name, si.age, ss.score "
                + "FROM student_infos si "
                + "JOIN student_scores ss "
                + "ON si.name=ss.name "
                + "WHERE ss.score>=80");

        spark.sql("DROP TABLE IF EXISTS good_student_infos");
        // 根据DataFrame创建临时表
        goodStudentsDF.registerTempTable("goodstudent_temp");
        Dataset result = spark.sql("select * from goodstudent_temp");
        result.show();

        /**
         * 将临时视图保存到hive表 good_student_infos
         */
        goodStudentsDF.write().mode(SaveMode.Overwrite).saveAsTable("good_student_infos2");

        spark.table("good_student_infos2").foreach(row -> {
            //  两种方式获取每行的数据
            System.out.println(row.get(2));
            System.out.println(row.getInt(row.fieldIndex("score")));
        });


    }

}
