package com.anu.weather;


import com.anu.weather.utils.AverageValue;
import com.anu.weather.utils.WeatherRecordParser;
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

/*
添加Combiner步骤，实现求每年的平均气温
 */
public class Weather_exer06 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Weather_exer05.class);

        //设置Mapper和Reducer
        job.setMapperClass(Weather_exer06Mapper.class);
        job.setCombinerClass(Weather_exer06Reducer.class);
        job.setReducerClass(Weather_exer06Reducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(AverageValue.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/Weather/input"));
        Path outpath = new Path("C:/MR/Weather/output6");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }

        FileOutputFormat.setOutputPath(job, outpath);

        boolean f = job.waitForCompletion(true);

        if (f) {
            System.out.println("job任务结束");
        } else {
            System.out.println("执行失败！");
        }
    }

    static class Weather_exer06Mapper extends Mapper<LongWritable,Text,IntWritable,AverageValue> {

        private WeatherRecordParser parser = new WeatherRecordParser();

        private IntWritable year = new IntWritable();
        private IntWritable temp = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            parser.parser(value.toString());
            if(parser.isValid()) {
                year.set(parser.getYear());
                temp.set(parser.getTemperature());
            }
            context.write(year, new AverageValue(1,temp.get()));
        }
    }

    static class Weather_exer06Combiner extends Reducer<IntWritable,AverageValue,IntWritable,AverageValue> {
    }

    static class Weather_exer06Reducer extends Reducer<IntWritable,AverageValue,IntWritable,AverageValue> {

        @Override
        protected void reduce(IntWritable key, Iterable<AverageValue> values, Context context) throws IOException, InterruptedException {

            int num = 0;
            double avgTem = 0;

            for (AverageValue av:values) {
                num += av.getNum();
                avgTem+=av.getAvg();
            }

            avgTem = avgTem/num;
            context.write(key,new AverageValue(num,avgTem));
        }

    }

}
