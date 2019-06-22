package com.wugui.sparkstarter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

/**
 * spark老版本支持hive的使用方法
 * 2.0spark已经不推荐使用hiveContext，改用SparkSession进行统一
 **/
public class SparkHiveOldVersion {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("hive");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

//HiveContext是SQLContext的子类。
        HiveContext hiveContext = new HiveContext(sc);
        hiveContext.sql("USE default");
        hiveContext.sql("show tables").show();
        hiveContext.sql("DROP TABLE IF EXISTS student_infos");
//在hive中创建student_infos表
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING,age INT) row format delimited fields terminated by ',' ");
        hiveContext.sql("load data local inpath '/Users/huzekang/study/spark-starter/src/main/resources/student_infos.txt' into table student_infos");

        hiveContext.sql("DROP TABLE IF EXISTS student_scores");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT) row format delimited fields terminated by ','");
        hiveContext.sql("LOAD DATA "
                + "LOCAL INPATH '/Users/huzekang/study/spark-starter/src/main/resources/student_scores.txt'"
                + "INTO TABLE student_scores");

        hiveContext.sql("select * from student_infos").show();
        hiveContext.sql("select * from student_scores").show();
/**
 * 查询表生成DataFrame
 */
        Dataset goodStudentsDF = hiveContext.sql("SELECT si.name, si.age, ss.score "
                + "FROM student_infos si "
                + "JOIN student_scores ss "
                + "ON si.name=ss.name "
                + "WHERE ss.score>=80");

        hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");

        goodStudentsDF.registerTempTable("goodstudent");
        Dataset result = hiveContext.sql("select * from goodstudent");
        result.show();

/**
 * 将结果保存到hive表 good_student_infos
 */
        goodStudentsDF.write().mode(SaveMode.Overwrite).saveAsTable("good_student_infos");

//        Row[] goodStudentRows = hiveContext.table("good_student_infos").collect();
//        for(Row goodStudentRow : goodStudentRows) {
//            System.out.println(goodStudentRow);
//        }
        sc.stop();
    }

}
