package com.anu.recom_03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;

public class Recommend02 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setMapperClass(Recommend02Mapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        TextInputFormat.setInputPaths(job,new Path("C:/MR/Recommand/output_command_comout/"));

        Path outpath = new Path("C:/MR/Recommand/output_command_comout_02/");
        FileSystem fileSystem = FileSystem.get(conf);
            if(fileSystem.exists(outpath)) {
            fileSystem.delete(outpath,true);
        }
            TextOutputFormat.setOutputPath(job,outpath);

            if (job.waitForCompletion(true)){
            System.out.println("job执行结束！");
        } else {
            System.out.println("job执行失败！");
        }
    }


    public static class Recommend02Mapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            if(split.length == 2) {
                context.write(new Text(split[0]),new IntWritable(Integer.parseInt(split[1])));
            }
        }
    }
}
