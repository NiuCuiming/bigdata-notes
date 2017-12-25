package com.anu.findFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OurFriends1 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(OurFriends.class);
        job.setMapperClass(OurFriends1Mapper.class);
        job.setReducerClass(OurFriends1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("C:/MR/Friends/output/"));
        Path path = new Path("C:/MR/Friends/output1/");

        //获取文件系统，删除存在的输出目录
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,path);

        if (job.waitForCompletion(true)){
            System.out.println("job执行结束！");
        } else {
            System.out.println("job执行失败！");
        }
    }


    static class OurFriends1Mapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //输入 <C-C,B>
            String[] split = value.toString().split(",");
            context.write(new Text(split[0].substring(1)),new Text(split[1].charAt(0)+""));
        }
    }

    static class OurFriends1Reducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            for (Text text : values) {
                sb.append(text).append(" ");
            }

            context.write(key,new Text(sb.toString()));
        }
    }


}
