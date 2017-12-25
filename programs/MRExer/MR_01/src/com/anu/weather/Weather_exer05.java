package com.anu.weather;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 每一年每个气象站检测到的最高气温
 */
public class Weather_exer05 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Weather_exer05.class);

        //设置Mapper和Reducer
        job.setMapperClass(Weather_exer05Mapper.class);
        job.setReducerClass(Weather_exer05Reducer.class);

        //map输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/Weather/input"));
        Path outpath =new Path("C:/MR/Weather/output_year_station_max");

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


    public static class Weather_exer05Mapper extends Mapper<LongWritable,Text,Text,Text> {

        private String FileName;

        /**
         * 获取文件名
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            FileSplit split = (FileSplit)context.getInputSplit();
            String name = split.getPath().getName();
            int index = name.lastIndexOf("-");
            FileName = name.substring(index+1);

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //读取一行获取气象站和温度
            String line = value.toString();
            String stationNum = line.substring(0,15);
            String temperature = line.substring(87,92);

            //输出 年份:气象站 温度
            context.write(new Text(FileName+":"+stationNum),new Text(temperature));

        }
    }

    public static class Weather_exer05Reducer extends Reducer<Text,Text,Text,IntWritable> {

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
