package com.anu.libimseti;

import com.anu.libimseti.utils.LibimsetiParser;
import com.anu.weather.Weather_exer01;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 1，	利用MR编程模型计算每个用户打出的平均分
 */
public class Libimseti_erex01 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Weather_exer01.class);

        //设置Mapper和Reducer
        job.setMapperClass(Libimseti_erex01Mapper.class);
        job.setReducerClass(Libimseti_erex01Reducer.class);

        //Map和Reduce输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/libimseti/input"));
        Path outpath =new Path("C:/MR/libimseti/output_01");

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

    static class Libimseti_erex01Mapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        private LibimsetiParser parser = new LibimsetiParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            parser.parser(line);
            context.write(new Text(parser.getUserID()),new IntWritable(parser.getRating()));
        }
    }

    static class Libimseti_erex01Reducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int num = 0;
            int sum = 0;
            double avg = 0;

            for (IntWritable rate:values) {
                num ++;
                sum += rate.get();
            }

            avg = sum/num;
            context.write(key,new DoubleWritable(avg));

        }
    }
}
