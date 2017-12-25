package com.anu.recom_05;

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

public class AverageRate_cluster {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("mapred.jar", "out/artifacts/avg_rate/avg_rate.jar");
        Job job = Job.getInstance(conf);

        job.setMapperClass(AverageRateMapper.class);
        job.setReducerClass(AverageRateReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job,new Path("MR/Recommand/result/"));
        Path outpath = new Path("MR/Recommand/AverageRate/");

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




    static class AverageRateMapper extends Mapper<LongWritable,Text,Text,Text> {

        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            k.set("1");
            v.set(split[1]);
            context.write(k,v);
        }
    }

    static class AverageRateReducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Text k = new Text();
            Text v = new Text();

            int count = 0;
            double avgCover = 0;
            double avgCorre = 0;

            for (Text text: values) {
                count++;
                String[] split = text.toString().split(" ");
                avgCorre += Double.parseDouble(split[0]);
                avgCover += Double.parseDouble(split[1]);
            }

            avgCorre /= count;      //准确率
            avgCover /= count;      //覆盖率

            k.set("平均准确率："+avgCorre);
            v.set("平均覆盖率："+avgCover);
            context.write(k,v);
            
        }
    }
}
