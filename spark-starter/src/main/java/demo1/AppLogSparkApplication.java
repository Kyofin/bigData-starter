package demo1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class AppLogSparkApplication {
    static DBHelper db1 = null;
    public static void main(String[] args) throws SQLException {
        //1.创建spark配置文件和上下文对象
        SparkConf conf = new SparkConf().setAppName("sparkTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        //2.读取日志文件并创建一个RDD，使用SparkContext的textFile（）方法
        JavaRDD<String> javaRDD = sc.textFile("./app_log.txt");

        //将RDD映射成为key-value格式，为后面的reducebykey聚合做准备。
        JavaPairRDD<String, AccessLogInfo> accessLogPairRdd = mapAccessLogRDD2Pair(javaRDD);
        //根据deviceID返回聚合后的结果
        JavaPairRDD<String, AccessLogInfo> aggregateLogPairRDD = aggregateByDeviceId(accessLogPairRdd);

        //将按照deviceID聚合的key映射为二次排序的key,value映射为deviceID
        JavaPairRDD<AccessLogSortKey, String> accessLogSortRDD = mapRDDkey2SortKey(aggregateLogPairRDD);
        ///实现降序排序
        JavaPairRDD<AccessLogSortKey, String> sortedAccessLogRDD= accessLogSortRDD.sortByKey(false);
        //获取前 top 10
        List<Tuple2<AccessLogSortKey, String>> top10DataList = sortedAccessLogRDD.take(10);
        //创建dbhelp对象
        db1 = new DBHelper();

        String sql  = "insert into spark(deviceId,upTraffic,downTraffic,timeStamp) values(?,?,?,?)";

        //打印前top 10
        for(Tuple2<AccessLogSortKey, String> data :  top10DataList){
//            System.out.println(data._2 +" "+data._1.getUpTraffic());
            PreparedStatement pt  = db1.conn.prepareStatement(sql);
            pt.setString(1,data._2);
            pt.setString(2,data._1.getUpTraffic()+"");
            pt.setString(3,data._1.getDownTraffic()+"");
            pt.setString(4,data._1.getTimestamp()+"");
            //注意让pt执行
            pt.executeUpdate();
        }

        //关闭上下文
        sc.close();

    }
    //将日志的RDD映射为key-value的格式
    private static JavaPairRDD<String, AccessLogInfo>  mapAccessLogRDD2Pair(JavaRDD<String> javaRDD){
        //PairFunction中第一个string表示的是传入的参数，后面两个代表返回值javaRDD
        return javaRDD.mapToPair(new PairFunction<String, String, AccessLogInfo>() {

            private static  final  long  serivaVersionUID = 1L;
            @Override
            //进行一行一行的读取
            public Tuple2<String, AccessLogInfo> call(String javaRDD) throws Exception {
                //根据\t进行切分
                String[] accessLogSplited = javaRDD.split("\t");
                //获取四个字段
                long timestamp = Long.valueOf(accessLogSplited[0]);
                String deviceID = accessLogSplited[1];
                long upTraffic = Long.valueOf(accessLogSplited[2]);
                long downTraffic = Long.valueOf(accessLogSplited[3]);
                // 将时间戳，上行流量和下行流量封装为自定义的可序列化对象
                AccessLogInfo accessLogInfo = new AccessLogInfo(timestamp,upTraffic,downTraffic);
                return new Tuple2<String, AccessLogInfo>(deviceID,accessLogInfo);
            }
        });

    }
    /**
     * 根据deviceID进行聚合求出上行和下行的流量，及其最早访问的时间
     */
    private static JavaPairRDD<String, AccessLogInfo> aggregateByDeviceId(JavaPairRDD<String, AccessLogInfo> accessLogPairRdd){
        //Function2的前两个accessLogInfo对应call的前两个，第三个是返回的
        return accessLogPairRdd.reduceByKey(new Function2<AccessLogInfo, AccessLogInfo, AccessLogInfo>() {
            private static  final  long  serivaVersionUID = 1L;
            @Override
            public AccessLogInfo call(AccessLogInfo accessLogInfo1, AccessLogInfo accessLogInfo2) throws Exception {
                long timestamp = accessLogInfo1.getTimestamp() < accessLogInfo2.getTimestamp()?accessLogInfo1.getTimestamp():accessLogInfo2.getTimestamp();
                long upTraffic = accessLogInfo1.getUpTraffic()+accessLogInfo2.getUpTraffic();
                long downTraffic=accessLogInfo1.getDownTraffic()+accessLogInfo2.getDownTraffic();
                //进行聚合之后产生一个AccessLogInfo
                AccessLogInfo accessLogInfo = new AccessLogInfo(timestamp,upTraffic,downTraffic);
                return accessLogInfo;
            }
        });
    }

    /**
     * 将RDD的key映射为二次排序的key
     */
    private  static  JavaPairRDD<AccessLogSortKey,String> mapRDDkey2SortKey(JavaPairRDD<String, AccessLogInfo> aggregateLogPairRDD){
        //后两个为返回的参数
      return  aggregateLogPairRDD.mapToPair(new PairFunction<Tuple2<String, AccessLogInfo>, AccessLogSortKey,String>() {
          private static  final  long  serivaVersionUID = 1L;
            @Override
            //tuple的key是deviceID，value是AccessLogInfo
            public Tuple2<AccessLogSortKey,String> call(Tuple2<String, AccessLogInfo> tuple ) throws Exception {
                String deviceID= tuple._1;
                AccessLogInfo accessLogInfo = tuple._2;
                AccessLogSortKey accessLogSortKey = new AccessLogSortKey(accessLogInfo.getTimestamp(),accessLogInfo.getUpTraffic(),accessLogInfo.getDownTraffic());
                //new 出去一个新的Tuple,这时候key变成了二次排序的key
                return new Tuple2<AccessLogSortKey,String>(accessLogSortKey,deviceID);
            }
        });
    }
}

















































