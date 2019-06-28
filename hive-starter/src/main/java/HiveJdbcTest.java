/**
 * 如果频繁出现下面错误，试试更换hive目录下的lib目录下的mysql驱动
 * Error while compiling statement: FAILED: SemanticException Unable to fetch table hive_table1. Could not retrieve transation read-only status server
 */

import java.sql.*;

public class HiveJdbcTest {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
   
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        // 设置hive 的jdbc连接
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
        Statement stmt = con.createStatement();
        String tableName = "big_data_table";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName +  " (key int, value string)");
        System.out.println("Create table success!");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("=======Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
 
        // describe table
        sql = "describe " + tableName;
        System.out.println("=======Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println("表字段名："+res.getString(1) + "\t" +"表字段类型："+ res.getString(2));
        }
 

        sql = "select * from " + tableName;
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getInt(1) + "\t" + res.getString(2));
        }

        // 插数据到hive_table1
        sql = "insert into  " + tableName + " values (22,'xxded')";
        System.out.println("=======Running: " + sql);
        stmt.executeUpdate( sql);

        // 查询聚合操作
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

    }
}