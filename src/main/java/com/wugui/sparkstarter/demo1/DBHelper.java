package com.wugui.sparkstarter.demo1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBHelper {
    public  static  final String url  = "jdbc:mysql://192.168.5.148:3306/dataset";
    public  static  final String driver  = "com.mysql.jdbc.Driver";
    public  static  final String user = "root";
    public  static  final String password ="eWJmP7yvpccHCtmVb61Gxl2XLzIrRgmT";

    //获取数据库链接
    public Connection conn = null;
    public DBHelper(){
        try{
            Class.forName(driver );
            conn = DriverManager.getConnection(url,user,password);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void close(){
        try {
            this.conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}





































