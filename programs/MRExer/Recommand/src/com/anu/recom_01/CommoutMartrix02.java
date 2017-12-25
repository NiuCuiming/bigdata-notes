package com.anu.recom_01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CommoutMartrix02 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setMapperClass(CommoutMartrix02Mapper.class);
        job.setReducerClass(CommoutMartrix02Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        TextInputFormat.setInputPaths(job,new Path("C:/MR/Recommand/output_wupin_comout/"));
        Path outpath = new Path("C:/MR/Recommand/output_wupin_comout02/");
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


    public static class CommoutMartrix02Mapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        static String one = null;
        static String two = null;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t")[1].split(" ");

            for (int i = 0; i < split.length; i++) {
                for (int j = 0; j < split.length; j++) {

                    /*one = split[i].compareTo(split[j]) >= 0?split[j]:split[i];
                    two = one.equals(split[i])?split[j]:split[i];*/

                    context.write(new Text(split[i]+":"+split[j]),new IntWritable(1));
                }
                
            }
        }
    }

    public static class CommoutMartrix02Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable one:values) {
                count++;
            }
            context.write(key,new IntWritable(count));
        }
    }



}
