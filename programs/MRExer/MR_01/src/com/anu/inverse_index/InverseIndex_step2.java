package com.anu.inverse_index;

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

public class InverseIndex_step2 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //在本地测试好像没影响
        //job.setJarByClass(InverseIndex_step2.class);

        //设置Mapper和Reducer
        job.setMapperClass(InverseIndex_step2Mapper.class);
        job.setReducerClass(InverseIndex_step2Reducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/InverseIndex/output"));
        Path outpath =new Path("C:/MR/InverseIndex/output1");

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

    public static class InverseIndex_step2Mapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            context.write(new Text(split[0]),new Text(split[1]));
        }
    }

    public static class InverseIndex_step2Reducer extends Reducer<Text,Text,Text,Text> {

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
