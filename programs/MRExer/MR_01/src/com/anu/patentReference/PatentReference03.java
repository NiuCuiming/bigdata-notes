package com.anu.patentReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.Sampler;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 在上一个测试的基础上
 * 测试实现：
 * 全局排序
 */
public class PatentReference03 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Configuration con = new Configuration();

        con.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, ",");

        Job job = Job.getInstance(con);

        //设置Mapper，Reducer
        ChainMapper.addMapper(job, InverseMapper.class,
                Text.class,Text.class, Text.class, Text.class, con);

        ChainMapper.addMapper(job,PatentReference02.PatentReference02Mapper.class,
                Text.class,Text.class,Text.class,IntWritable.class,con);


        ChainReducer.setReducer(job,IntSumReducer.class,
                Text.class,IntWritable.class,Text.class,IntWritable.class,con);

        //设置输入输出路径
        KeyValueTextInputFormat.setInputPaths(job, new Path("C:/MR/Patent/input/small.txt")); //测试小文件
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        Path outpath =new Path("C:/MR/Patent/output03");
        FileSystem fs = FileSystem.get(con);
        if(fs.exists(outpath)){
            fs.delete(outpath, true);
        }
        TextOutputFormat.setOutputPath(job,outpath);
        job.setOutputFormatClass(TextOutputFormat.class);

        /*
        实现全局排序
         */
        //设置Reducer个数
        job.setNumReduceTasks(4);
        //设置分区器(思考为什么必须设置)
        job.setPartitionerClass(TotalOrderPartitioner.class);
        //构建采样器
        Sampler<DoubleWritable,Text> sampler =
                new RandomSampler<DoubleWritable, Text>(1,1000,10);

        // 运行采样器，计算每个键对应的分布区间，
        // 并将计算结果保存
        InputSampler.writePartitionFile(job,sampler);

        // 获得采样器分析的分区结果文件路径名
        URI partFileURI=
                new URI(TotalOrderPartitioner
                        .getPartitionFile(con));

        // 将分区结果分发到集群各节点,Map任务将根据分区文件对数据分区
        job.addCacheFile(partFileURI);


        //开始运行
        boolean f= job.waitForCompletion(true);

        if(f){
            System.out.println("job任务结束");
        }else {
            System.out.println("执行失败！");
        }
    }
}
