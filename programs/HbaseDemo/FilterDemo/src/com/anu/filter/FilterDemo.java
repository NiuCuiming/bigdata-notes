package com.anu.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class FilterDemo {

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
            table = connection.getTable(TableName.valueOf("person"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void showScanner(ResultScanner resultScanner) {

        for(Result row:resultScanner) {

            for (KeyValue kv: row.raw()) {
                System.out.print(new String(kv.getRow())+" ");
                System.out.print(new String(kv.getFamily())+ ":");
                System.out.print(new String(kv.getQualifier())+"=");
                System.out.print(new String(kv.getValue())+" ");
                System.out.println("timeStamp:"+kv.getTimestamp());
            }
        }

        System.out.println("over!");
    }

    /**
     * 过滤器列表
     */
    @Test
    public void filterList() throws IOException {

        //过滤器搭载在scan之上，san可以搭载过滤器，也可以搭载过滤器集合
        Scan scan = new Scan();
        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = null;

        //1.过滤器列表，搭建在scan描述类上，table 获取 ResultScanner
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        //过滤掉home_info:address 值为shanghai的行，和school_info 值为shanghai的hang
        filterList.addFilter(new SingleColumnValueExcludeFilter(Bytes.toBytes("home_info"),Bytes.toBytes("address"),
                CompareFilter.CompareOp.EQUAL,Bytes.toBytes("shanghai")));
        filterList.addFilter(new SingleColumnValueExcludeFilter(Bytes.toBytes("school_info"),Bytes.toBytes("address"),
                CompareFilter.CompareOp.EQUAL,Bytes.toBytes("shanghai")));
        //scan.setFilter(filterList);

        //2.通过正则表达式过滤掉 列族 home_info:address 值以shang开头的行。
        RegexStringComparator rsc = new RegexStringComparator("shang.");
        singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes("home_info"), Bytes.toBytes("address"),
                CompareFilter.CompareOp.EQUAL, rsc);
        //scan.setFilter(singleColumnValueExcludeFilter);

        //SubStringComparator
        //用于监测一个子串是否存在于值中，并且不区分大小写
        //示例：过滤掉school_info:address 包含 an 的行。
        SubstringComparator ssc = new SubstringComparator("an");
        singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes("school_info"), Bytes.toBytes("address"),
                CompareFilter.CompareOp.EQUAL, ssc);
        //scan.setFilter(singleColumnValueExcludeFilter);

        //BinaryComparator
        //二进制比较器，用于按字典顺序比较 Byte 数据值。
        // 为什么没出来key为002的值？？
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("taiyuan"));
        singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes("school_info"), Bytes.toBytes("address"),
                CompareFilter.CompareOp.EQUAL, bc);
        //scan.setFilter(singleColumnValueExcludeFilter);

        //BinaryPrefixComparator
        //前缀二进制比较器。与二进制比较器不同的是，只比较前缀是否相同。
        // 为什么没出来key为002的值？？
        BinaryPrefixComparator bpc = new BinaryPrefixComparator(Bytes.toBytes("tai"));
        singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes("school_info"), Bytes.toBytes("address"),
                CompareFilter.CompareOp.EQUAL, bpc);
        //scan.setFilter(singleColumnValueExcludeFilter);


        //SingleColumnValueExcludeFilter
        //跟 SingleColumnValueFilter 功能一样，只是不查询出该列的值。
        //过滤过程。
        SingleColumnValueExcludeFilter sce = new SingleColumnValueExcludeFilter(Bytes.toBytes("home_info"),Bytes.toBytes("address"),
                CompareFilter.CompareOp.EQUAL,Bytes.toBytes("shanghai"));
        //scan.setFilter(sce);

        ResultScanner scanner = table.getScanner(scan);
        showScanner(scanner);
    }

    /**
     * 测试二
     */
    @Test
    public void filterTest() throws IOException {

        //过滤器搭载在scan之上，san可以搭载过滤器，也可以搭载过滤器集合
        Scan scan = new Scan();

        //SingleColumnValueFilter:列值过滤器，下面示例过滤留下home_info:address = shanghai 的row
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("home_info"),
                Bytes.toBytes("address"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("shanghai"));

        //列值排除过滤器，和上面过滤器相反，过滤排除 home_info:address = shanghai 的行
        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes("home_info"),
                Bytes.toBytes("address"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("shanghai"));
        /**
         * 结果：
         * 0002 school_info:address=shanghai timeStamp:1513149581179
         * 0003 home_info:numberCount=4 timeStamp:1513149594707
         * 0003 school_info:address=taiyuan timeStamp:1513149594707
         */

        singleColumnValueFilter.setFilterIfMissing(false);
        scan.setFilter(singleColumnValueExcludeFilter);
        ResultScanner scanner = table.getScanner(scan);
        showScanner(scanner);

    }

}
