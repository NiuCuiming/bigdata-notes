package com.anu.workFlow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;

public class PatentReferenceSort_workFlow {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        //设置InverseKayValue反转Mapper分割符
//        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, ",");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        //设置工作流的输入目录，临时目录，输出目录
        Path input= new Path("C:/MR/Patent/input/small.txt");
        Path temp=  new Path("C:/MR/Patent/workFlow_temp");
        if(fs.exists(temp)){
            fs.delete(temp, true);
        }
        Path output=new Path("C:/MR/Patent/workFlow_output");
        if(fs.exists(output)){
            fs.delete(output, true);
        }
        //作业配置
        Job job_1 = Job.getInstance(conf);

        //设置作业一的Mapper
        ChainMapper.addMapper(job_1, InverseMapper.class,Text.class, Text.class,Text.class, Text.class,conf);
        ChainMapper.addMapper(job_1, WcCountMapper.class,Text.class, Text.class,Text.class,IntWritable.class,conf);

        //设置作业一的Reducer
        job_1.setReducerClass(IntSumReducer.class);

        //设置作业一的输入输出格式
        KeyValueTextInputFormat.setInputPaths(job_1,input);
        job_1.setInputFormatClass(KeyValueTextInputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job_1,temp);
        job_1.setOutputFormatClass(SequenceFileOutputFormat.class);

        //设置输出类型
        job_1.setOutputKeyClass(Text.class);
        job_1.setOutputValueClass(IntWritable.class);

        //---------------------------------------------------------------------------------------------------------------


        //作业二的配置
        Job job_2 = Job.getInstance(conf);
        job_2.setMapperClass(InverseMapper.class);

        //设置作业二的MR
        ChainReducer.setReducer(
                job_2,Reducer.class,
                IntWritable.class,Text.class,IntWritable.class,Text.class,conf);
        ChainReducer.addMapper(
                job_2,InverseMapper.class,IntWritable.class,Text.class,Text.class,IntWritable.class,conf);

        //设置作业二的输入输出路径
        SequenceFileInputFormat.addInputPath(job_2,temp);
        job_2.setInputFormatClass(
                SequenceFileInputFormat.class);

        TextOutputFormat.setOutputPath(job_2,output);

        //因为job_2的Map Reduce输出不一致
        job_2.setMapOutputKeyClass(IntWritable.class);
        job_2.setMapOutputValueClass(Text.class);
        job_2.setOutputKeyClass(Text.class);
        job_2.setOutputValueClass(IntWritable.class);


        /**
         * 配置工作流>>>>>
         * 工作流的节点是一个个ControlledJob的实例，通过这些实例可以配置不同ControlledJob依赖
         * 这些实例可以被JobControl统一在一块
         */
        ControlledJob controlledJob_1=
                new ControlledJob(
                        job_1.getConfiguration());
        controlledJob_1.setJob(job_1);

        ControlledJob controlledJob_2=
                new ControlledJob(
                        job_2.getConfiguration());
        controlledJob_2.setJob(job_2);

        //配置依赖关系
        controlledJob_2.addDependingJob(
                controlledJob_1);

        //工作流实例
        JobControl jobControl = new JobControl("my frist");
        jobControl.addJob(controlledJob_1);
        jobControl.addJob(controlledJob_2);


        /*
        public class JobControl implements Runnable
        工作流对象，是一个Runnable实现对象
        按照线程的方式开启
         */
        new Thread(jobControl).start();
        //job_1.waitForCompletion(true);

        //汇报工作状态
       while(true){
            for(ControlledJob cj:jobControl.getRunningJobList()){
                cj.getJob().monitorAndPrintJob();
            }
            if(jobControl.allFinished()) break;
        }

    }

    static class WcCountMapper extends Mapper<Text, Text,Text,IntWritable> {

        private IntWritable one=new IntWritable(1);

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            context.write(key,one);
        }
    }

}
