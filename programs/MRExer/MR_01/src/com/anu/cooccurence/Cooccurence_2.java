package com.anu.cooccurence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Cooccurence_2 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        //在本地测试好像没影响
        //job.setJarByClass(InverseIndex_step1.class);

        //设置Mapper和Reducer
        job.setMapperClass(Cooccurence_2Mapper.class);
        job.setReducerClass(Cooccurence_2Reducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/cooccurence/output"));
        Path outpath =new Path("C:/MR/cooccurence/output1");

        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(outpath)){
            fs.delete(outpath, true);
        }

        FileOutputFormat.setOutputPath(job, outpath);

        boolean f= job.waitForCompletion(true);

        if(f){
            System.out.println("job任务结束");
        }else {
            System.out.println("执行失败！");
        }
    }

    static class Cooccurence_2Mapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] peoples = value.toString().split(",");
            String[] friends = peoples[1].split(" ");


            for (int i = 0; i < friends.length; i++) {

                for (int j = i+1; j < friends.length; j++) {

                    if (friends[i].compareTo(friends[j])<=0){
                        context.write(new Text(friends[i]+","+friends[j]),new IntWritable(1));
                    } else {
                        context.write(new Text(friends[i]+","+friends[j]),new IntWritable(1));
                    }
                }
            }
        }
    }

    static class Cooccurence_2Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int counts = 0;
            for (IntWritable count: values) {
                counts++;
            }
            context.write(key,new IntWritable(counts));
        }
    }
}
