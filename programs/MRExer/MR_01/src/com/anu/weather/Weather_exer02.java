package com.anu.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
 以/data/weather/999999-99999-1992数据，请计算出每个气象站检测到的平均气温
 */
public class Weather_exer02 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Weather_exer01.class);

        //设置Mapper和Reducer
        job.setMapperClass(Weather_exer01.Weather_exer01Mapper.class);
        job.setReducerClass(Weather_exer02.Weather_exer02Reducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/Weather/input"));
        Path outpath =new Path("C:/MR/Weather/output_avg");
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


    static class Weather_exer02Reducer extends Reducer<Text,Text,Text,LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int temp = 0;
            int tempCount = 0;
            int recordCounts = 0;

            for (Text temperature : values) {

                //+0240  +0190 +0167
                String s = temperature.toString();
                temp = Integer.parseInt(s.substring(1));

                if(s.charAt(0) == '-') {
                    temp = 0-temp;
                }

                tempCount += temp;

                recordCounts++;
            }

            context.write(key,new LongWritable((tempCount/recordCounts)));

        }
    }
}
