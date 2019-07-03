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
import java.util.Properties;

/**
 *
 * 使用Spark2.0的SparkSession进行统一api操作
 * Spark2.0中只有一个入口点(spark会话),从中你可以获得各种其他入口点(spark上下文,流上下文等)
 *
 * @author huzekang*/
public class SparkSessionStarter {
    public static void main(String[] args) {
//        在Spark的早期版本，sparkContext是进入Spark的切入点。我们都知道RDD是Spark中重要的API，然而它的创建和操作得使用sparkContext提供的API；
//        对于RDD之外的其他东西，我们需要使用其他的Context。
//        比如对于流处理来说，我们得使用StreamingContext；对于SQL得使用sqlContext；而对于hive得使用HiveContext。
//        然而DataSet和Dataframe提供的API逐渐称为新的标准API，我们需要一个切入点来构建它们，所以在 Spark 2.0中我们引入了一个新的切入点(entry point)：SparkSession。
//        SparkConf、SparkContext和SQLContext都已经被封装在SparkSession当中.
//　　     SparkSession实质上是SQLContext和HiveContext的组合（未来可能还会加上StreamingContext），所以在SQLContext和HiveContext上可用的API在SparkSession上同样是可以使用的。
//        SparkSession内部封装了sparkContext，所以计算实际上是由sparkContext完成的。
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

//        quickStart(spark);
//
//        customCode(spark);
//
//        reflectionCode(spark);

        runJdbcDatasetExample(spark);

    }

    /**
     * 快速测试例子
     */
    public static void quickStart(SparkSession spark) {
        Dataset<Row> df = spark.read().text("/Users/huzekang/study/bigdata-starter/spark-starter/src/main/resources/students.txt");
        df.show();
    }


    /**
     * 使用SparkSql读pg并写入pg
     */
    private static void runJdbcDatasetExample(SparkSession spark) {
        // $example on:jdbc_dataset$
        // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
        // Loading data from a JDBC source
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://192.168.1.150:5432/postgres")
                .option("dbtable", "public.person")
                .option("user", "postgres")
                .option("password", "123456")
                .load();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "123456");
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:postgresql://192.168.1.150:5432/postgres", "public.person", connectionProperties);

        // Saving data to a JDBC source
        jdbcDF.write()
                .format("jdbc")
                .option("url", "jdbc:postgresql://192.168.1.150:5432/postgres")
                .option("dbtable", "public.person_jdbcDF")
                .option("user", "postgres")
                .option("password", "123456")
                .save();

        jdbcDF2.write()
                .jdbc("jdbc:postgresql://192.168.1.150:5432/postgres", "public.person_jdbcDF2", connectionProperties);


    }


    /**
     *
     * 文件 => JavaRDD => DataFrame
     * 使用反射机制推断RDD的数据结构（不推荐） :
     *   当spark应用可以推断RDD数据结构时，可使用这种方式。这种基于反射的方法可以使代码更简洁有效。
     */
    public static void reflectionCode(SparkSession spark) {
        // Create an RDD of Person objects from a text file
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("/Users/huzekang/study/bigdata-starter/spark-starter/src/main/resources/people.txt")
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
     *   当spark应用无法推断RDD数据结构时，可使用这种方式。（推荐）
     */
    public static void customCode(SparkSession spark) {
        // Create an RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("/Users/huzekang/study/bigdata-starter/spark-starter/src/main/resources/people.txt", 1)
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
