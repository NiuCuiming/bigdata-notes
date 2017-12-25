package com.anu.recom_04;

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

public class RecommendUnique {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setMapperClass(RecommendUniqueMapper.class);
        job.setReducerClass(RecommendUniqueReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        TextInputFormat.setInputPaths(job,new Path("C:/MR/Recommand/output_command_comout_02/"),
                new Path("C:/MR/Recommand/input/"));

        Path outpath = new Path("C:/MR/Recommand/RecommandUnique/");
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


    public static class RecommendUniqueMapper extends Mapper<LongWritable,Text,Text,Text> {

        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            if(line.contains(",")) {
                String[] split = line.split("\t");
                k.set(split[0]);
                v.set(split[1]);
                context.write(k,v);
            } else {
                String[] split = line.split(" ");
                if(split.length == 3) {
                    k.set(split[0] + "," + split[1]);
                    v.set(split[2]);
                    context.write(k,v);
                }
            }
        }
    }

    public static class RecommendUniqueReducer extends Reducer<Text,Text,Text,Text> {

        Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            for (Text text:values) {
                v = text;
                count++;
            }

            if(count > 1) {
                return;
            } else {
                context.write(key,v);
            }
        }
    }
}
