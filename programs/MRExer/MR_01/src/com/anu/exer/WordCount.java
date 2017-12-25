package com.anu.exer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        //把业务逻辑相关的信息（哪个是mapper，哪个是reducer，要处理的数据在哪里，输出的结果放哪里。。。。。。）描述成一个job对象      //把这个描述好的job        //

        //job.setOutputKeyClass和job.setOutputValueClas在默认情况下是同时设置map阶段和reduce阶段的输出，也就是说只有map和reduce输出是一样的时候才不会出问题。
        //当map和reduce输出是不一样的时候就需要通过job.setMapOutputKeyClass和job.setMapOutputValueClas来设置map阶段的输出。


        //获取配置文件，通过配置文件构造job对象
        Configuration configuration = new Configuration();


        //显示设定一些参数，下面的设置也是默认的设置,这样可以在本地模拟mr运行
//        configuration.set("fs.defaultFS", "file://localhost");
//        configuration.set("mapreduce.framework.name", "local");

        //显示设定一些参数，调用hdfs的文件,在本地执行
        //configuration.set("fs.deFaultFS","hdfs://node6:9000");
        //configuration.set("yarn.resourcemanager.hostname","local");

        Job job = Job.getInstance(configuration);

        //指定要执行任务的类
        job.setJarByClass(WordCount.class);

        //设置job的mapper和reducer
        job.setMapperClass(WCMapper.class);
        job.setCombinerClass(WCReducer.class);
        job.setReducerClass(WCReducer.class);

        //设置Map输出的类型，错误点！！！
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputKeyClass(IntWritable.class);

//      设置任务输出的kv类型(reducer的输出类型)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //执行处理数据所在位置
        FileInputFormat.setInputPaths(job,new Path("C:/weblog/input"));
        Path outputPath = new Path("C:/weblog/output1");

//        FileInputFormat.setInputPaths(job,new Path("hdfs://node6:9000/wc/input"));
//        Path outputPath = new Path("hdfs://node6:9000/wc/output/");

        //获取文件系统,这种方法可以删除成功
//        FileSystem fs =FileSystem.get(new URI("hdfs://node6:9000"), configuration, "root");
        FileSystem fs =FileSystem.get(configuration);
        //这种方法删除不成功
//        FileSystem fs =FileSystem.get(configuration);
        //如果输出目录存在，自动删除
        if(fs.exists(outputPath)) {
            fs.delete(outputPath,true);
        }
        FileOutputFormat.setOutputPath(job,outputPath);

        //提交任务
        boolean b = job.waitForCompletion(true);
        if(b) {
            System.out.println("job执行成功!");
        }

    }




    static class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            /*
            获取文件的每一行，按照空格切分
             */
            String line = value.toString();
            String[] words = line.split(" ");

            /*
            把每一个word当作key，1当作值，写出
             */
            for (String word:words) {

                Text text = new Text(word);
                IntWritable intWritable = new IntWritable(1);
                context.write(text, intWritable);
            }
        }


    }

    static class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            /*获取每个单词的values，
            对每个单词的values求和*/

            int counts = 0;
            for (IntWritable count: values) {
                counts += count.get();
            }
            /*写出key和求和值*/
            context.write(key,new IntWritable(counts));

        }
    }
}
