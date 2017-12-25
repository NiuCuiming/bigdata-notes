package com.anu.weather;


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
1.以/data/weather/999999-99999-1992数据，请计算出每个气象站检测到的最高气温

需要：key： 一行数据，找出气象站编号 为 key [0,15)
     value：这条记录检测的温度           [87,92)
     007899999999999	+0195

 */
public class Weather_exer01 {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Weather_exer01.class);

        //设置Mapper和Reducer
        job.setMapperClass(Weather_exer01Mapper.class);
        job.setReducerClass(Weather_exer01Reducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/Weather/input"));
        Path outpath =new Path("C:/MR/Weather/output_max");

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

    static class Weather_exer01Mapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String stationNum = line.substring(0,15);
            String temperature = line.substring(87,92);

            //获取气象站和温度数据，输出结果
            context.write(new Text(stationNum),new Text(temperature));

        }
    }

    static class Weather_exer01Reducer extends Reducer<Text,Text,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Integer temp = null;
            int maxTem = 0;

            for (Text temperature : values) {

                //+0240  +0190 +0167
                String s = temperature.toString();
                temp = Integer.parseInt(s.substring(1));

                if(s.charAt(0) == '-') {
                    temp = 0-temp;
                }

                if(temp != 9999 && temp > maxTem) {
                    maxTem =temp;
                }
            }

            context.write(key,new IntWritable(maxTem));

        }
    }
}
