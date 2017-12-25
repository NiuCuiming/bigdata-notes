package com.anu.recom_01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CommoutMartrix01 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setMapperClass(CommoutMartrix01Mapper.class);
        job.setReducerClass(CommoutMartrix01Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        TextInputFormat.setInputPaths(job,new Path("C:/MR/Recommand/input/"));
        Path outpath = new Path("C:/MR/Recommand/output_wupin_comout/");
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


    public static class CommoutMartrix01Mapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split(" ");
            if(split.length == 3) {
                context.write(new Text(split[0]),new Text(split[1]));
            }
        }
    }

    public static class CommoutMartrix01Reducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            for (Text text:values) {
                sb.append(text.toString()+" ");
            }

            context.write(key,new Text(sb.toString()));
        }
    }

}
