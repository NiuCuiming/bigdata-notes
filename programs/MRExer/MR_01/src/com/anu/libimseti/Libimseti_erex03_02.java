package com.anu.libimseti;

import com.anu.weather.Weather_exer01;
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

public class Libimseti_erex03_02 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Weather_exer01.class);

        //设置Mapper和Reducer
        job.setMapperClass(Libimseti_erex03_02Mapper.class);
        job.setReducerClass(Libimseti_erex03_02Reducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/libimseti/output_03_1"));
        Path outpath =new Path("C:/MR/libimseti/output_03_2");

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

    static class Libimseti_erex03_02Mapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] peoples = (value.toString().split("\t")[1]).split(" ");

            for (int i = 0; i < peoples.length; i++) {
                for (int j = i+1; j < peoples.length; j++) {

                    if(peoples[i].compareTo(peoples[j])>=0) {
                        context.write(new Text(peoples[i]+"--"+peoples[j]),new IntWritable(1));
                    } else {
                        context.write(new Text(peoples[j]+"--"+peoples[i]),new IntWritable(1));
                    }

                }
            }
        }
    }

    static class Libimseti_erex03_02Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable count:values) {
                sum ++;
            }

            context.write(key,new IntWritable(sum));

        }
    }
}
