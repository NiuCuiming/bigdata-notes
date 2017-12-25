package com.anu.findFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OurFriends {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(OurFriends.class);
        job.setMapperClass(OurFriendsMapper.class);
        job.setReducerClass(OurFriendsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("C:/MR/Friends/input/"));
        Path path = new Path("C:/MR/Friends/output/");

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

    static class OurFriendsMapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] peoples = line.split(":");
            String people = peoples[0];
            String[] friends = peoples[1].split(",");

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < friends.length; i++) {
                context.write(new Text(friends[i]),new Text("<"+friends[i]+","+people+"> "));
            }
        }
    }


    static class OurFriendsReducer extends Reducer<Text,Text,Text,NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> list = new ArrayList<>();

            //获取拥有共同好友的人
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                String s = iterator.next().toString();
                list.add(s.split(",")[1].charAt(0)+"");
            }


           //组合，输出
            for (int i = 0; i < list.size(); i++) {
                for (int j = i+1; j < list.size(); j++) {
                    context.write(new Text("<"+list.get(i)+"-"+list.get(j)+","+key+">"),NullWritable.get());
                }
            }
        }
    }


}
