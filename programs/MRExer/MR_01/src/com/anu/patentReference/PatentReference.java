package com.anu.patentReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class PatentReference {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置job的工作类jar包
        job.setJarByClass(PatentReference.class);

        //设置Mapper和Reducer
        job.setMapperClass(PatentMapper.class);
        job.setReducerClass(PatentReducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置读取原始信息的格式信息以及输出到HDFS集群中的格式信心
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置输入输出路径
        TextInputFormat.addInputPath(job,new Path("C:/MR/Patent/input"));
        TextOutputFormat.setOutputPath(job,new Path("C:/MR/Patent/output"));
//        TextInputFormat.addInputPath(job,new Path(args[0]));
//        TextOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交作业
        int num = job.waitForCompletion(true)?0:1;

        if (num == 0) {
            System.out.println("执行成功！");
        }
    }

    public static class PatentMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        private PatentParaer parser = new PatentParaer();
        private Text key = new Text();
        private IntWritable value = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            parser.parse(value);
            if (parser.isValid()) {
                this.key.set(parser.getRefPatentID());
                context.write(this.key,this.value);
            }

        }
    }

    public static class PatentReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable iw:values) {
                count+=iw.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

}
