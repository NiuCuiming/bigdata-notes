package com.anu.Sort;

import com.anu.weather.Weather_exer01;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/*
在reduce端实现二次排序
 */
public class SecondSort {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Weather_exer01.class);

        //设置Mapper和Reducer
        job.setMapperClass(SecondSortMapper.class);
        job.setReducerClass(SecondSortReducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/Sort/input"));
        Path outpath =new Path("C:/MR/Sort/output_01");

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

    static class SecondSortMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split(" ");
            context.write(new Text(split[0]),new IntWritable(Integer.parseInt(split[1])));

        }
    }

    static class SecondSortReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            List<Integer> list = new ArrayList<>();
            for (IntWritable value: values) {
                list.add(value.get());
            }
            Collections.sort(list);

            for (Integer value:list) {
                context.write(key,new IntWritable(value));
            }

        }
    }

}
