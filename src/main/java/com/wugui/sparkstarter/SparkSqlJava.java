package com.wugui.sparkstarter;


import lombok.Data;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @program: spark-starter
 * @author: huzekang
 * @create: 2019-06-20 21:46
 **/
public class SparkSqlJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

//        quickStart(spark);

//        code(spark);

        reflection(spark);

    }

    /**
     * 快速测试例子
     */
    public static void quickStart(SparkSession spark) {
        Dataset<Row> df = spark.read().text("/Users/huzekang/study/spark-starter/src/main/resources/students.txt");
        df.show();
    }


    /**
     *
     * 文件 => JavaRDD => DataFrame
     * 使用反射机制推断RDD的数据结构 :
     *   当spark应用可以推断RDD数据结构时，可使用这种方式。这种基于反射的方法可以使代码更简洁有效。
     */
    public static void reflection(SparkSession spark) {
        // Create an RDD of Person objects from a text file
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("/Users/huzekang/study/spark-starter/src/main/resources/people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = spark.sql("SELECT name ,age FROM people WHERE age BETWEEN 13 AND 19");
        teenagersDF.show();
//        +------+---+
//        |  name|age|
//        +------+---+
//        |Justin| 19|
//        +------+---+

        // 写出df到指定文件
        // 参考例子：![](https://i.loli.net/2019/06/22/5d0e1eac239d177427.png)
        teenagersDF.select("age").write().mode(SaveMode.Append).format("parquet").save("tmp/person_age");

        // The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+

        // or by field name
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+
    }


    /**
     * 通过编程接口构造一个数据结构，然后映射到RDD上
     *   当spark应用无法推断RDD数据结构时，可使用这种方式。
     */
    public static void code(SparkSession spark) {
        // Create an RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("/Users/huzekang/study/spark-starter/src/main/resources/people.txt", 1)
                .toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "name age";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) line -> {
            String[] attributes = line.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT name ,age  FROM people");
        results.show();
//        +-------+---+
//        |   name|age|
//        +-------+---+
//        |Michael| 29|
//        |   Andy| 30|
//        | Justin| 19|
//        +-------+---+

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
        // +-------------+
        // |        value|
        // +-------------+
        // |Name: Michael|
        // |   Name: Andy|
        // | Name: Justin|
        // +-------------+
    }

    @Data
    public static class Person {

        private String name;
        private Integer age;

    }
}
