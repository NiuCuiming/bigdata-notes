package com.anu.weather;


import com.anu.weather.utils.WeatherRecordParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 每一年，每个气象站的平均气温
 */
public class Weather_exer07 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Weather_exer01.class);

        //设置Mapper和Reducer
        job.setMapperClass(Weather_exer07Mapper.class);
        job.setReducerClass(Weather_exer07Reducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/Weather/input"));
        Path outpath =new Path("C:/MR/Weather/output7");

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


    static class Weather_exer07Mapper extends Mapper<LongWritable,Text,Text,DoubleWritable> {

        WeatherRecordParser parser = new WeatherRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            parser.parser(value.toString());

            context.write(new Text(parser.getYear()+":"+parser.getStationId())
                    ,new DoubleWritable(parser.getTemperature()));

        }
    }

    static class Weather_exer07Reducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double avgTem = 0;
            int num = 0;
            for (DoubleWritable tem:values) {
                num++;
                avgTem = tem.get();
            }
            avgTem = avgTem/num;
            context.write(key,new DoubleWritable(avgTem));
        }
    }
}
