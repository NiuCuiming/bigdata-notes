package com.anu.patentReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;


/**
 * 测试使用：
 * 利用ChainMapper,ChainReducer实现Mapper的链式执行
 * 利用KeyValueTextInputFormat读取数据
 * 利用hadoop内置Mapper和内置Reducer
 */
public class PatentReference02 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration con = new Configuration();

        //设置key,value,分隔符,下面两种方法都生效
        con.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        //con.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, ",");

        Job job = Job.getInstance(con);

        //ChainMapper,设置job的Mapper，可以连接多个Mapper会依次执行，注意前输出和后输入的keyvalue的类型要一致
        //InverseMapper.class，内置Mapper：接收key，value值，然后两者反转，输出value,key
        ChainMapper.addMapper(job, InverseMapper.class,
                Text.class,Text.class, Text.class, Text.class, con);

        ChainMapper.addMapper(job,PatentReference02Mapper.class,
                Text.class,Text.class,Text.class,IntWritable.class,con);


        //ChainReducer，设置job的Reducer，可以“设置”一个Reducer,添加多个Mapper，注意前输出和后输入的keyvalue的类型要一致
        ChainReducer.setReducer(job,IntSumReducer.class,
                Text.class,IntWritable.class,Text.class,IntWritable.class,con);

        ChainReducer.addMapper(job, InverseMapper.class,
                Text.class,IntWritable.class,IntWritable.class,Text.class,con);

        //设置KeyValueTextInputFormat的输入路径
        KeyValueTextInputFormat.setInputPaths(job, new Path("C:/MR/Patent/input"));
        //设置指定的InputFormatClass，默认是TextInputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        Path outpath =new Path("C:/MR/Patent/output02");
        FileSystem fs = FileSystem.get(con);
        if(fs.exists(outpath)){
            fs.delete(outpath, true);
        }
        TextOutputFormat.setOutputPath(job,outpath);
        //设置指定的OutputFormatClass，默认是OutputFormatClass，所以下面行可以不设置
        //job.setOutputFormatClass(TextOutputFormat.class);


        boolean f= job.waitForCompletion(true);

        if(f){
            System.out.println("job任务结束");
        }else {
            System.out.println("执行失败！");
        }
    }

    static class PatentReference02Mapper extends Mapper<Text,Text,Text,IntWritable> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key,new IntWritable(1));
        }
    }
}
