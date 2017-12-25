package com.anu.recom_05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class LocalSort {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(LocalSortMapper.class);
        job.setReducerClass(LocalSortReducer.class);
       // job.setSortComparatorClass(MyComparator.class);
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        TextInputFormat.setInputPaths(job,new Path("C:/MR/Recommand/output/"));
        Path outpath = new Path("C:/MR/Recommand/sorted/");

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

    static class LocalSortMapper extends Mapper<LongWritable,Text,IntWritable,Text> {

        IntWritable k = new IntWritable();
        Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String user;       //用户
            String rerate;     //推荐次数

            String[] split = value.toString().split("\t");
            user = split[0].split(",")[0];
            rerate = split[1];

            k.set(-(Integer.parseInt(user+rerate)));
            v.set(split[0].split(",")[1]);

            context.write(k,v);
        }
    }

    static class LocalSortReducer extends Reducer<IntWritable,Text,Text,Text> {

        Text k = new Text();
        Text v = new Text();
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            sb.append("R ");
            int i =(int) (key.get() / 10);
            k.set(-i+"");
            for (Text good: values) {
                sb.append(good.toString()+" ");
            }

            v.set(sb.toString());
            context.write(k,v);
        }
    }


    /*class MyComparator extends WritableComparator {

        public MyComparator(){

            super(IntWritable.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {

            IntWritable i = (IntWritable)a;
            IntWritable j = (IntWritable)b;

            return -(i.compareTo(j));
        }
    }*/

    static class MyGroupingComparator extends WritableComparator {

        protected MyGroupingComparator() {

            super(IntWritable.class, true);
        }

        public int compare(WritableComparable a, WritableComparable b) {

            int i =(int) (((IntWritable)a).get())/10;
            int j =(int) (((IntWritable)b).get())/10;

            //将ProduceId相同的bean都视为相同，从而聚合为一组
            return i-j;
        }

    }
}