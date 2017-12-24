package com.anu.hive2jdbc;


import java.sql.*;

public class Hive2JdbcDemo {

    //连接hive的驱动包
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";


    public static void main(String[] args) throws SQLException {

        try {

            //加载驱动包，如果加载失败，退出程序
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        //获取hive的连接
        Connection con = (Connection) DriverManager.getConnection("jdbc:hive2://node03:10000/default","root","");

        //创建语句
        Statement stmt = con.createStatement();

        //创建数据库
        //stmt.execute("create database anu");

        //查看表
        /*ResultSet resultSet = stmt.executeQuery("show tables");
        if(resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }*/

        //加载数据到hive表中
        String filepath = "text.txt";
        stmt.execute("load data local inpath '" + "/home/anu/test.txt" + "' into table " + "test");


        con.close();

        System.out.println("over");
    }

}
