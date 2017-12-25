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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SingleFriend {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJobName("SingleFriend");
        job.setJarByClass(SingleFriend.class);

        //设置Mapper和Reducer
        job.setMapperClass(SingleFriendMapper.class);
        job.setReducerClass(SingleFriendReducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("C:/MR/Friends/input1"));
        Path outpath =new Path("C:/MR/Friends/output_single");

        FileSystem fs = FileSystem.get(conf);
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

    static class SingleFriendMapper extends Mapper<LongWritable,Text,Text,Text> {

        String name;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            //获取分片的文件名
            name = ((FileSplit)context.getInputSplit()).getPath().getName();
            System.out.println(name);

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String line = value.toString();
            String[] peoples = line.split(",");

            //获取行，比较，写出
            String man1 = peoples[0].trim().compareTo(peoples[1].trim())>0?peoples[0].trim():peoples[1].trim();
            String man2 = man1.equals(peoples[0].trim())?peoples[1].trim():peoples[0].trim();

            //按照大的输出
            context.write(new Text(man1),new Text(man2));

        }
    }

    static class SingleFriendReducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Set<String> friends = new HashSet<>();

            for (Text friend : values) {
                friends.add(friend.toString());
            }
            for (String friend:friends) {
                context.write(key,new Text(friend));
            }
        }
    }
}
