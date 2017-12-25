package com.anu.crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CrudDemo01 {

    //与连接相关的变量
    static private Configuration configuration;
    static private HBaseAdmin hBaseAdmin;
    static private Connection connection;
    static private Table table;

    static {

        //配置文件
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","node01,node02,node03");
        configuration.set("hbase.zookeeper.property","2181");


        //获取操作的表和表管理类
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
            connection = ConnectionFactory.createConnection(configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一个表
     */
    @Test
    public void createTable() throws IOException {

        //表描述类，列族描述类，组合，hbaseAdmin创建表

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("person"));
        HColumnDescriptor hcd1 = new HColumnDescriptor("home_info");
        HColumnDescriptor hcd2 = new HColumnDescriptor("school_info");

        htd.addFamily(hcd1);
        htd.addFamily(hcd2);

        hBaseAdmin.createTable(htd);

        System.out.println("建表成功！");
    }

    /**
     * 插入单条数据
     */
    @Test
    public void insertSingleData() throws IOException {

        //表，put，把put放入表
        Table person = connection.getTable(TableName.valueOf("person"));
        Put row1 = new Put(Bytes.toBytes("0001"));
        row1.add(Bytes.toBytes("home_info"),Bytes.toBytes("address"),Bytes.toBytes("shangxiProvince"));
        person.put(row1);
        System.out.println("insert success！");
    }

    /**
     * 插入多条数据
     */
    @Test
    public void insertBatchData() throws IOException {

        //表，list<put>, 把list放入表中
        Table person = connection.getTable(TableName.valueOf("person"));

        List<Put> listPut = new ArrayList<>();

        Put row1 = new Put(Bytes.toBytes("0005"));
        row1.add(Bytes.toBytes("home_info"),Bytes.toBytes("numberCount"),Bytes.toBytes("4"));
        row1.add(Bytes.toBytes("school_info"),Bytes.toBytes("address"),Bytes.toBytes("taiyuan"));

        Put row2 = new Put(Bytes.toBytes("0006"));
        row2.add(Bytes.toBytes("home_info"),Bytes.toBytes("address"),Bytes.toBytes("shanghai"));
        row2.add(Bytes.toBytes("school_info"),Bytes.toBytes("address"),Bytes.toBytes("shanghai"));

        listPut.add(row1);listPut.add(row2);

        person.put(listPut);

        System.out.println("insert success!");
    }

    /**
     * 单条查询
     */
    @Test
    public void getSingleRecord() throws IOException {

        //表，Get：存放记录，Result
        Table person = connection.getTable(TableName.valueOf("person"));

        Get get0001 = new Get(Bytes.toBytes("0001"));
        Result row0001 = person.get(get0001);

        for (KeyValue kv: row0001.raw()) {
            System.out.print(new String(kv.getRow())+" ");
            System.out.print(new String(kv.getFamily())+ ":");
            System.out.print(new String(kv.getQualifier())+"=");
            System.out.print(new String(kv.getValue())+" ");
            System.out.println("timeStamp:"+kv.getTimestamp());
        }

        System.out.println("over!");
    }

    /**
     * 批量查询
     * 测试失败程序卡住了
     */
    @Test
    public void getBatchRecord() throws IOException {

        //表，scan, 通过表获得scan描述的ResultScanner，遍历
        Table person = connection.getTable(TableName.valueOf("person"));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("0001"));
        scan.setStopRow(Bytes.toBytes("0004"));

        ResultScanner resultScanner = person.getScanner(scan);

        //----从这儿开始执行卡住了程序----
        for(Result row:resultScanner) {

            for (KeyValue kv: row.raw()) {
                System.out.print(new String(kv.getRow())+" ");
                System.out.print(new String(kv.getFamily())+ ":");
                System.out.print(new String(kv.getQualifier())+"=");
                System.out.print(new String(kv.getValue())+" ");
                System.out.println("timeStamp:"+kv.getTimestamp());
            }

        }
    }

    /**
     * 扫描表
     * 测试失败，程序又卡住了。
     */
    @Test
    public void scan() throws IOException {

        Table person = connection.getTable(TableName.valueOf("person"));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("home_info"), Bytes.toBytes("address"));
        scan.setStartRow(Bytes.toBytes("0001"));
        scan.setStopRow(Bytes.toBytes("0003"));

        ResultScanner scanner = person.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("home_info"),Bytes.toBytes("address"))));
        }

    }

    /**
     * 删除数据
     */
    @Test
    public void deleteRecord() throws IOException {

        //表，delete描述，表调用delete(delete描述)
        Table person = connection.getTable(TableName.valueOf("person"));

        Delete delete = new Delete(Bytes.toBytes("0004"));
        person.delete(delete);

        System.out.println("去查询吧...");
    }

    /**
     * 删除表
     * 调用失败，卡住了又
     */
    @Test
    public void deleteTable() throws IOException {

        //hbaseAdmin 删除表
        hBaseAdmin.disableTable("test");
        hBaseAdmin.deleteTable("test");
    }


}
