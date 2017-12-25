package com.anu.recom_05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ResultNumber {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(ResultNumberMapper.class);
        job.setReducerClass(ResultNumberReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job,new Path("C:/MR/Recommand/sorted/"),
                new Path("C:/MR/Recommand/after/"));
        Path outpath = new Path("C:/MR/Recommand/result/");

        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outpath)) {
            fileSystem.delete(outpath,true);
        }
        TextOutputFormat.setOutputPath(job,outpath);

        if (job.waitForCompletion(true)){
            System.out.println("job执行结束！");
        } else {
            System.out.println("job执行失败！");
        }
    }

    static class ResultNumberMapper extends Mapper<LongWritable,Text,Text,Text> {

        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] split = null;
            if(line.contains("\t")) {
                split = line.split("\t");
            } else {
                split = line.split(" ");
            }
            k.set(split[0]);
            v.set(split[1]);

            context.write(k,v);
        }
    }

    static class ResultNumberReducer extends Reducer<Text,Text,Text,Text> {

        /*Text k = new Text();
        Text v = new Text();*/

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //推荐结果数目
            double recomNumber = 0;
            //结果数目
            double resultNubmer = 0;
            //交集数目
            double interNubmer = 0;

            //存放推荐的set
            Set<String> recomSet = new HashSet<>();
            //存放结果的set
            Set<String> resultSet = new HashSet<>();

            for (Text result: values) {
                String s = result.toString();
                if(s.contains("R")) {
                    String[] split = s.split(" ");
                    for (int i = 1; i < split.length; i++) {
                        recomSet.add(split[i]);
                    }
                } else {
                    resultSet.add(s);
                }

            }

            recomNumber = recomSet.size();
            resultNubmer = resultSet.size();
            resultSet.retainAll(recomSet);
            interNubmer = resultSet.size();


            //输出格式：准确率，覆盖率
            context.write(key,new Text((interNubmer/recomNumber)+" "
                    + (interNubmer/resultNubmer)));
        }
    }
}


