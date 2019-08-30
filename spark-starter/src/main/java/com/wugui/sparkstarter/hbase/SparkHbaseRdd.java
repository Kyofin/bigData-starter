package com.wugui.sparkstarter.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;

/**
 * spark 原生连接 hbase
 * 参考资料：http://dblab.xmu.edu.cn/blog/1094-2/
 *
 * @program: bigdata-starter
 * @author: huzekang
 * @create: 2019-08-30 16:58
 **/
public class SparkHbaseRdd {

    public static void main(String[] args) throws IOException {
        String tableName = "FileTable";
        SparkSession sc = SparkSession.builder().appName("SparkHBaseRDD").master("local[1]").getOrCreate();
        Configuration hbaseConf = HBaseConfiguration.create();
        //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
        hbaseConf.set("hbase.zookeeper.quorum", "cdh01");
        //设置zookeeper连接端口，默认2181
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName);


        // 如果表不存在，则创建表
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);
        if (!admin.isTableAvailable(tableName)) {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(tableDesc);
        }
        //读取数据并转化成rdd
        RDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD = sc.sparkContext()
                .newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hBaseJavaRdd = hBaseRDD.toJavaRDD();
        hBaseJavaRdd.foreach(v1 -> {
            //获取行键
            String key = new String(v1._2.getRow());
            //通过列族和列名获取列
            String name = new String(v1._2.getValue("fileInfo".getBytes(), "name".getBytes()));
            String type = new String(v1._2.getValue("fileInfo".getBytes(), "type".getBytes()));
            System.out.println("Row key:" + key + "\tfileInfo.Name:" + name + "\tfileInfo.type:" + type);
        });

        admin.close();

        sc.stop();
    }
}
