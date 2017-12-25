package com.anu.block;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.mapred.JobClient;

import java.net.URI;
import java.util.List;


public class BlockOpreatoin {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node6:9000"),conf,"root");

        HdfsDataInputStream open = (HdfsDataInputStream)fileSystem.open(new Path("/wc/input/b.txt"));


        List<LocatedBlock> allBlocks = open.getAllBlocks();
        for (LocatedBlock locatedBlock : allBlocks) {
            ExtendedBlock block = locatedBlock.getBlock();

            System.out.println("块ID："+block.getBlockId());
            System.out.println("块名字："+block.getBlockName());

            System.out.println("块开始："+locatedBlock.getStartOffset());

            DatanodeInfo[] locations = locatedBlock.getLocations();

            System.out.println("获取当前数据块所在dataNode的信息！");
            for (DatanodeInfo datanodeInfo:locations) {
                System.out.println("Name:"+datanodeInfo.getHostName());
                System.out.println("Add："+datanodeInfo.getInfoAddr());
                System.out.println("版本："+datanodeInfo.getSoftwareVersion());
            }

        }
    }
}
