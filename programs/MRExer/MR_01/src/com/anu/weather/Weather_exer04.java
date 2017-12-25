package com.anu.weather;
import java.io.IOException;

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


/*
求最大气温值
 */
public class Weather_exer04 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJobName("Weather_01_Anu");
        job.setJarByClass(Weather_exer01.class);

        //设置Mapper和Reducer
        job.setMapperClass(Weather_exer04Mapper.class);
        job.setReducerClass(Weather_exer04Reducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("C:/MR/Weather/input"));
        Path outpath =new Path("C:/MR/Weather/output_year_max");

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

    static class Weather_exer04Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private WeatherRecordParser parser = new WeatherRecordParser();
        private IntWritable year = new IntWritable();
        private IntWritable temp = new IntWritable();


        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {

            parser.parser(value.toString());

            if(parser.isValid()) {
                year.set(parser.getYear());
                temp.set(parser.getTemperature());
            }
            context.write(year, temp);
        }

    }

    static class Weather_exer04Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        protected void reduce(IntWritable arg0, Iterable<IntWritable> arg1,
                              Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context arg2)
                throws IOException, InterruptedException {

            int max = Integer.MIN_VALUE;
            for (IntWritable v : arg1) {
                if(v.get()>max) max = v.get();
            }
            arg2.write(arg0, new IntWritable(max));
        }

    }

}
