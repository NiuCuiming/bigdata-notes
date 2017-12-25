package com.anu.Compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.*;

public class CompressDemo {

    /*
    测试使用gzip把日期数据压缩，写在新的目录里面
     */
    public static void main(String[] args) throws IOException {
        release();

    }

    /*
    解码
     */
    public static void release() throws IOException {

        //获取文件系统客户端
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);


        //获取文件流
        InputStream is = new FileInputStream("C:/MR/Weather/ComOutput/1990.gzip");
        OutputStream os = new FileOutputStream(new File("C:/MR/Weather/ComOutput/1990"));

        //获取编码解码器
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec gzipCodec = factory.getCodecByName("gzip");  //根据文件的后缀获取到相应的Codec

        //连接
        CompressionInputStream cis = gzipCodec.createInputStream(is);

        //读写
        IOUtils.copyBytes(cis,os,4096);

    }

    /*
    编码
     */
    public static void compree() throws IOException {

        //获取文件系统客户端
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);


        //获取文件流
        InputStream is = new FileInputStream("C:/MR/Weather/input/999999-99999-1990");
        OutputStream os = new FileOutputStream(new File("C:/MR/Weather/ComOutput/1990.gzip"));

        //获取编码解码器
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec gzipCodec = factory.getCodecByName("gzip");

        //连接
        CompressionOutputStream cos = gzipCodec.createOutputStream(os);

        //读写
        IOUtils.copyBytes(is,cos,4096);

        cos.close();
        is.close();
    }

}
