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
                                //  如果需要作业要以jar包形式提交到remote spark，则使用spark://host:port
//                .master("spark://10.0.0.50:7077")

                                //  如果idea中测试则使用local。
                                //  如果作业要以jar包形式提交到yarn则不设置master。
                .master("local")

                .appName("Java Spark SQL Starter ！！")
                .enableHiveSupport()
                                // 改变spark sql写出时使用的压缩编码。
                                // 默认是snappy，可能会在用hive客户端查询时出现错误：
                                // Caused by: org.xerial.snappy.SnappyError: [FAILED_TO_LOAD_NATIVE_LIBRARY] null
                .config("spark.sql.parquet.compression.codec", "gzip")
                .getOrCreate();

        spark.sql("USE default");
        spark.sql("show tables").show();
        //  在hive中创建student_infos表
        spark.sql("DROP TABLE IF EXISTS student_infos");
        spark.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING,age INT) row format delimited fields terminated by ',' ");
        //  加载本地文件数据到hive表中
        spark.sql("load data local inpath '/Users/huzekang/study/bigdata-starter/spark-starter/src/main/resources/student_infos.txt' into table student_infos");

        //  在hive中创建student_scores表
        spark.sql("DROP TABLE IF EXISTS student_scores");
        spark.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT) row format delimited fields terminated by ','");
        //  加载本地文件数据到hive表中
        spark.sql("LOAD DATA "
                + "LOCAL INPATH '/Users/huzekang/study/bigdata-starter/spark-starter/src/main/resources/student_scores.txt'"
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

        // 根据DataFrame创建临时表
        goodStudentsDF.registerTempTable("goodstudent_temp");
        Dataset result = spark.sql("select * from goodstudent_temp");
        result.show();

        /**
         * 将临时视图保存到hive表 good_student_infos
         */
        spark.sql("DROP TABLE IF EXISTS good_student_infos");
        goodStudentsDF.write().mode(SaveMode.Overwrite).saveAsTable("good_student_infos");

        spark.table("good_student_infos").foreach(row -> {
            //  两种方式获取每行的数据
            System.out.println(row.get(2));
            System.out.println(row.getInt(row.fieldIndex("score")));
        });


    }

}
