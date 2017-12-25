package com.anu.crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.net.URI;

public class Exer_01 {

    /*
    获取一个文件的所有block位置信息，然后读取指定block中的内容
     */

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node6:9000"), configuration, "ROOT");

        //获取到描述文件的信息,获取到描述文件块的信息
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/wc/input/b.txt"));


        //获取该文件所有的块信息
        BlockLocation[] fileBlockLocations =
                fileSystem.getFileBlockLocations(new Path("/wc/input/b.txt"),0,fileStatuses[0].getLen());

        for (BlockLocation blockLocation : fileBlockLocations) {

            System.out.println("块名字："+blockLocation.getNames());

            System.out.println("块的长度："+blockLocation.getLength());
            System.out.println("块的偏移量："+blockLocation.getOffset());

            //块的位置包括副本
            String[] hosts = blockLocation.getHosts();
            for (String host: hosts) {
                System.out.println("块副本的位置："+host);
            }
            //操作块,根据块的位置
            FSDataInputStream open = fileSystem.open(new Path("/wc/input/b.txt"));
            FileOutputStream fileOutputStream = new FileOutputStream("C:/weblog/input/block.txt");
            //跳转到该块的起始位置
            open.seek(blockLocation.getOffset());
            //复制到本地
            IOUtils.copyBytes(open,fileOutputStream,blockLocation.getLength(),true);
        }
    }
}
