package com.anu.libimseti;

import com.anu.libimseti.utils.LibimsetiParser;
import com.anu.weather.Weather_exer01;
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

public class Libimseti_erex03_01 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Weather_exer01.class);

        //设置Mapper和Reducer
        job.setMapperClass(Libimseti_erex03Mapper.class);
        job.setReducerClass(Libimseti_erex03Reducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/libimseti/input"));
        Path outpath =new Path("C:/MR/libimseti/output_03_1");

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

    static class Libimseti_erex03Mapper extends Mapper<LongWritable,Text,Text,Text> {

        private LibimsetiParser parser = new LibimsetiParser();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            parser.parser(line);
            context.write(new Text(parser.getUserID()), new Text(parser.getProfileID()));
        }
    }

    static class Libimseti_erex03Reducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();

            for (Text user:values) {
                sb.append(user.toString()+" ");
            }

            context.write(key,new Text(sb.toString()));
        }
    }
}
