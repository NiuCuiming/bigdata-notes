package com.anu.crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class CRUD {

    public static Configuration configuration;
    public static FileSystem fs;

    static {
        configuration = new Configuration();
        try {
            fs = FileSystem.get(new URI("hdfs://node6:9000"), configuration, "root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void MakeDeleteDir() throws IOException, URISyntaxException, InterruptedException {

        //创建文件夹
        fs.mkdirs(new Path("/wc/output1"));
        //删除文件夹
        fs.delete(new Path("/wc/output"),true);
        //重命名文件按
        fs.rename(new Path("/wc/output1"),new Path("/wc/output2"));
    }

    @Test
    public void UpDownFile() throws IOException {

        //本地待上传的文件按
//        Path src = new Path("C:/weblog/input/access.log.fensi");
//        fs.copyFromLocalFile(src,new Path("hdfs://node6:9000/wc/upFile/"));

        //下载文件
        Path dest = new Path("C:/weblog/down/access.log");
        fs.copyToLocalFile(new Path("hdfs://node6:9000/wc/upFile"),dest);
    }

    @Test
    public void List() throws IOException {

        //查看目录信息
        //返回一个LocatedFileStatus迭代器对象
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);


        while(listFiles.hasNext()) {

            //描述文件（不描述文件夹哦）的信息的接口：FileStatus，
            // LocatedFileStatus，包含文件位置文件信息描述类
            LocatedFileStatus fileStatus = listFiles.next();

            //通过上述对象，获取想要的文件信息
            System.out.println("****************************");
            System.out.println("文件名字："+fileStatus.getPath().getName());

            //获取文件块大小
            System.out.println("文件块大小："+fileStatus.getBlockSize()/(1024*1024)+"M");

            //文件权限
            System.out.println("文件的权限:"+fileStatus.getPermission());

            //文件长度
            System.out.println("文件长度:"+fileStatus.getLen()/(1024)+"K");


            //获取文件块，一个文件可能有多个块,每个块的副本,所以获取描述文件块的数组接收
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {

                //1.文件块的长度，起止位置
                System.out.println("文件的长度："+bl.getLength()+"文件的起止位置："+bl.getOffset());

                //2.获取文件块的主机名,一个文件块可能有多个副本，每个副本在不同的主机上
                String[] hosts = bl.getHosts();
                for (String host:hosts) {
                    System.out.println(host);
                }

                //3.获取名字列表(该文件块所在的 (主机:IP)s)
                String[] names = bl.getNames();
                for (String name:names) {
                    System.out.println(name);
                }
            }

        }
    }

    /*
    通过文件流访问hdfs
     */
    @Test
    public void IoCrud() throws IOException {

        /*
        相对封装好的方法，是更底层的操作方式
        操作hdfs文件的读取和写入流通常是 FSDataInputStream  FSDataOutputStream
        读写操作常用一个工具类叫做 IOUtils
         */


        /*
        1. 流操作，上传文件到hdfs
         */
        //创建一个目标是hdfs文件的输出流，注意其类型是FSDataOutputStream,创建方式是create
//        FSDataOutputStream outputStream = fs.create(new Path("/wc/input/b.txt"));
        //创建一个文件读取流
//        FileInputStream fileInputStream = new FileInputStream("C:/weblog/input/access.log.fensi");
//        IOUtils.copyBytes(fileInputStream,outputStream,4096);

        /*
        2. 定位文件的读取
         */
        //hdfs支持定位文件的读取，可以读文件从指定开始位置读取，且可以指定读取的长度

        //创建一个指向hdfs的读取流
//        FSDataInputStream open = fs.open(new Path("/wc/input/a.txt"));

        //创建一个指向本地的输出流
//        FileOutputStream fileOutputStream = new FileOutputStream("C:/weblog/input/a_down.txt");

        //下面的方式就是定位
//        open.seek(10);
        //复制，第三个参数指定复制的个数，最后一个指定是否关流
//        IOUtils.copyBytes(open,fileOutputStream,10L,true);


        /*
        3. 显示hdfs的文件内容
         */
        FSDataInputStream open = fs.open(new Path("/wc/input/a.txt"));
        IOUtils.copyBytes(open,System.out,10L,true);

        /*
        上述操作只是利用工具实现了简单的读写，每个流都有自己的read或write方法
         */

    }


    //写数据的练习 FSDataOutputStream
    @Test
    public void WriteExer() throws IOException, InterruptedException {

        //把一个本地文件写入到HDFS中
        InputStream in = new BufferedInputStream(new FileInputStream("C:/MR/FlowCount/input/flow.log"));

        FSDataOutputStream outputStream = fs.create(new Path("/exer/flow.log"), new Progressable() {
            @Override
            public void progress() {
                //自我解释，当这个输出流向目的写出数据时，这个过程就会被下面的语句报告
                System.out.println("写出数据...");
            }
        });

        System.out.println(outputStream.getPos());      //这个方法返回当前输出流的位置，所处二进制位的位置。
        IOUtils.copyBytes(in,outputStream,4096);
        System.out.println(outputStream.getPos());

        outputStream.close();
    }

    //获取文件或目录的元数据信息
    @Test
    public void getFileStatus() throws IOException {

        //列出文件
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fsta : fileStatuses) {
            System.out.println(fsta.getPath());
        }

        //获取描述文件的元数据信息的封装对象
        FileStatus fileStatus = fs.getFileStatus(new Path("/exer/flow.log"));

        //通过封装对象，详细列出元数据信息
        System.out.println("文件路径："+fileStatus.getPath());
        System.out.println("文件属主："+fileStatus.getOwner());
        System.out.println("文件属组："+fileStatus.getGroup());
        System.out.println("文件块大小："+fileStatus.getBlockSize());
        System.out.println("文件大小："+fileStatus.getLen());
        System.out.println("文件副本数："+fileStatus.getReplication());

        //描述文件块
    }

}
