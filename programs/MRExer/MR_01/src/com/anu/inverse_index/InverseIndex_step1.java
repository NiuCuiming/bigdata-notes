package com.anu.inverse_index;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 建立倒排索引表
 */
public class InverseIndex_step1 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //在本地测试好像没影响
        //job.setJarByClass(InverseIndex_step1.class);

        //设置Mapper和Reducer
        job.setMapperClass(InverseIndexMapper.class);
        job.setReducerClass(InverseIndexReducer.class);

        //Map和Reduce输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/InverseIndex/input"));
        Path outpath =new Path("C:/MR/InverseIndex/output");

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


    public static class InverseIndexMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        private String fileName;

        //获取文件名
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            fileName = inputSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获取每一个单词
            //输出每一个单词+文件名

            String line = value.toString();
            String[] words = line.split(" ");

            for (String word:words) {

                if(word.trim() == ""){
                    continue;
                }
                word = word+":"+fileName;
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }

    public static class InverseIndexReducer extends Reducer<Text,IntWritable,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            String[] split = key.toString().split(":");
            String word = split[0];
            String fileName = split[1];
            int counts = 0;

            for (IntWritable one:values) {
                counts+=1;
            }

            context.write(new Text(word),new Text(fileName+":"+counts));

        }
    }


}
