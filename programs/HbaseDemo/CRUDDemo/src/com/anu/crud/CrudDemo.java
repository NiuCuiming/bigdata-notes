package com.anu.crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class CrudDemo {

    //完善配置
    static private Configuration configuration;
    static private HBaseAdmin hBaseAdmin;
    static private Connection connection;
    static private Table table;


    /*
    hbase客户端直接和zookeeper打交道，所以需要知道一些管理Hbase的zookeeper的消息
     */
    static {

        //创建一个和Hbase有关的configuration
        configuration = HBaseConfiguration.create();

        //设置一些配置，连接到Hbase服务端
        configuration.set("hbase.zookeeper.quorum","node1,node2,node3");

        //设置zookeeper端口
        configuration.set("hbase.zookeeper.property","2181");

        //建立连接,实例化表管理类
        try {

            hBaseAdmin = new HBaseAdmin(configuration);
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf("user"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一个表，这个表没有任何region，
     */

    @Test
    public void createTable() throws IOException {

        //创建一个表管理类，当然管理Hbase数据库中表信息

        /*
        创建表
        1.表描述类 HTableDescriptor
         */

        //创建表的描述类,来描述要创建的表
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("user")); //建议使用参数是TableName的方法

        //创建列族描述类，表示创建表的列族信息
        HColumnDescriptor hcd1 = new HColumnDescriptor("info1");
        HColumnDescriptor hcd2 = new HColumnDescriptor("info2");

        //把列族描述添加到表描述中
        htd.addFamily(hcd1).addFamily(hcd2);

        //利用表管理类创建表
        hBaseAdmin.createTable(htd);
    }

    /**
     * 删除一个表
     * 先disabled，然后删除
     */
    @Test
    public void deleteTable() throws IOException {

        //注意执行这行时，必须要注意user表是enabled，对应的命令是deleteall
        hBaseAdmin.disableTable(TableName.valueOf("user"));
        hBaseAdmin.deleteTable(TableName.valueOf("user"));
        hBaseAdmin.close();
    }

    /**
     *插入单条数据
     */
    @Test
    public void insertData() throws IOException {

        //获取要插入数据的表,静态代码块中已经有了
        //表的插入方法table.put(); 参数有两个：
        //void put(Put var1)
        //void put(List<Put> var1)

        //插入数据的描述类
        Put put = new Put(Bytes.toBytes("141229"));  //参数是一个rowKey
        put.add(Bytes.toBytes("info1"),Bytes.toBytes("name"),Bytes.toBytes("Ann"));  //插入一个列

        table.put(put);

        //批量插入你知道该怎么做了吧
    }

    /**
     * 修改数据
     */
    @Test
    public void updateData() throws IOException {

        //修改数据也就是覆盖，行键不变数据添加上去就行
        Put put = new Put(Bytes.toBytes("141228"));
        put.add(Bytes.toBytes("info1"),Bytes.toBytes("name"),Bytes.toBytes("Amn"));  //插入一个列

        table.put(put);
    }

    /**
     * 删除数据
     */
    public void deleteData() throws IOException {

        //创建一个描述删除的对象
        Delete delete = new Delete(Bytes.toBytes("141228"));
        delete.addColumn(Bytes.toBytes("info1"),Bytes.toBytes("name"));

        table.delete(delete);

    }

    /**
     * 单条查询
     */
    @Test
    public void searchData() throws IOException {

        //同样的套路
        Get get = new Get(Bytes.toBytes("141230"));
        get.addColumn(Bytes.toBytes("info1"),Bytes.toBytes("name"));

        //注意会返回结果。
        Result result = table.get(get);

        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"),Bytes.toBytes("name"))));
    }

    /**
     * 全表扫描
     */
    @Test
    public void scanData() throws IOException {

        //创建全表扫描
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));

        scan.setStartRow(Bytes.toBytes("141228"));
        scan.setStopRow(Bytes.toBytes("141229"));

        ResultScanner scanner = table.getScanner(scan);


        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"),Bytes.toBytes("name"))));
        }

    }
}
