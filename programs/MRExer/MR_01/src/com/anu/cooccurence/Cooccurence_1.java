package com.anu.cooccurence;

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

public class Cooccurence_1 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        //在本地测试好像没影响
        //job.setJarByClass(InverseIndex_step1.class);

        //设置Mapper和Reducer
        job.setMapperClass(Cooccurence_1Mapper.class);
        job.setReducerClass(Cooccurence_1Reducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/cooccurence/input"));
        Path outpath =new Path("C:/MR/cooccurence/output");

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

    /*
    求共现好友次数，两两
     */

    static class Cooccurence_1Mapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] peoples = value.toString().split(",");
            context.write(new Text(peoples[0].trim()),new Text(peoples[1].trim()));

        }
    }

    static class Cooccurence_1Reducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            sb.append(",");
            for (Text text:values) {
                sb.append(text.toString()+" ");
            }

            context.write(key,new Text(sb.toString()));
        }
    }

}
