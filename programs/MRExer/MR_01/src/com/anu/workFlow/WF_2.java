package com.anu.workFlow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WF_2 extends Configured implements Tool {

    static class CounterMapper extends Mapper<
            Text,Text,
            Text,IntWritable>{

        private IntWritable one=new IntWritable(1);

        @Override
        protected void map(
                Text key,Text value,Context context) throws IOException, InterruptedException{
            context.write(key,one);
        }
    }

    @Override
    public int run(String[] args) throws Exception{
        Configuration conf=getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");

        Path input= new Path("C:/MR/Patent/input/small.txt");
        Path temp=  new Path("C:/MR/Patent/workFlow_temp");
        Path output=new Path("C:/MR/Patent/workFlow_output");

        // 作业1配置
        Job job_1=Job.getInstance(conf,this.getClass().getSimpleName());
        job_1.setJarByClass(this.getClass());

        ChainMapper.addMapper(
                job_1,InverseMapper.class,
                Text.class,Text.class,
                Text.class,Text.class,conf);

        ChainMapper.addMapper(
                job_1,CounterMapper.class,
                Text.class,Text.class,
                Text.class,IntWritable.class,conf);

        job_1.setReducerClass(IntSumReducer.class);
        job_1.setOutputKeyClass(Text.class);
        job_1.setOutputValueClass(IntWritable.class);

        job_1.setInputFormatClass(KeyValueTextInputFormat.class);
        job_1.setOutputFormatClass(
                SequenceFileOutputFormat.class);

        KeyValueTextInputFormat.addInputPath(job_1,input);
        SequenceFileOutputFormat.setOutputPath(job_1,temp);

        // 作业2配置
        Job job_2=Job.getInstance(conf,this.getClass().getSimpleName());
        job_2.setJarByClass(this.getClass());

        job_2.setMapperClass(InverseMapper.class);
        job_2.setMapOutputKeyClass(IntWritable.class);
        job_2.setMapOutputValueClass(Text.class);

        ChainReducer.setReducer(
                job_2,Reducer.class,
                IntWritable.class,Text.class,IntWritable.class,Text.class,conf);
        ChainReducer.addMapper(
                job_2,InverseMapper.class,IntWritable.class,Text.class,Text.class,IntWritable.class,conf);

        job_2.setInputFormatClass(
                SequenceFileInputFormat.class);
        job_2.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job_2,temp);
        TextOutputFormat.setOutputPath(job_2,output);

        ControlledJob controlledJob_1=
                new ControlledJob(
                        job_1.getConfiguration());
        controlledJob_1.setJob(job_1);

        ControlledJob controlledJob_2=
                new ControlledJob(
                        job_2.getConfiguration());
        controlledJob_2.setJob(job_2);

        controlledJob_2.addDependingJob(
                controlledJob_1);

        JobControl jobControl=
                new JobControl(
                        this.getClass().getSimpleName());
        jobControl.addJob(controlledJob_1);
        jobControl.addJob(controlledJob_2);

        new Thread(jobControl).start();

        while(true){
            for(ControlledJob cj:jobControl.getRunningJobList()){
                cj.getJob().monitorAndPrintJob();
            }
            if(jobControl.allFinished()) break;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new WF_2(),args));
    }
}
