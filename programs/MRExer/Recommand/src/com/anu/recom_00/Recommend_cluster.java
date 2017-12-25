package com.anu.recom_00;

import com.anu.recom_01.CommoutMartrix01;
import com.anu.recom_01.CommoutMartrix02;
import com.anu.recom_01.CommoutMartrix03;
import com.anu.recom_02.BuyMartrix01;
import com.anu.recom_03.Recommend01;
import com.anu.recom_03.Recommend02;
import com.anu.recom_04.RecommendUnique;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;

public class Recommend_cluster {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf= new Configuration();

        //输入和输出文件以及临时文件
        Path input= new Path("C:/MR/Recommand/input/");

        Path one_1_2 =  new Path("C:/MR/Recommand/one_1_2/");
        Path one_2_3 =  new Path("C:/MR/Recommand/one_2_3/");
        Path oneOutput =  new Path("C:/MR/Recommand/oneOutput/");
        Path twoOutput = new Path("C:/MR/Recommand/twoOutput/");
        Path three_1 = new Path("C:/MR/Recommand/three_1/");
        Path threeOutput = new Path("C:/MR/Recommand/threeOutput/");

        Path output=new Path("C:/MR/Recommand/output");

        //第一个job
        Job job_1 = Job.getInstance(conf);
        job_1.setMapperClass(CommoutMartrix01.CommoutMartrix01Mapper.class);
        job_1.setReducerClass(CommoutMartrix01.CommoutMartrix01Reducer.class);
        job_1.setOutputKeyClass(Text.class);
        job_1.setOutputValueClass(Text.class);
        TextInputFormat.setInputPaths(job_1,input);
        TextOutputFormat.setOutputPath(job_1,one_1_2);

        //第二个Job
        Job job_2 = Job.getInstance(conf);
        job_2.setMapperClass(CommoutMartrix02.CommoutMartrix02Mapper.class);
        job_2.setReducerClass(CommoutMartrix02.CommoutMartrix02Reducer.class);
        job_2.setOutputKeyClass(Text.class);
        job_2.setOutputValueClass(IntWritable.class);
        TextInputFormat.setInputPaths(job_2,one_1_2);
        TextOutputFormat.setOutputPath(job_2,one_2_3);

        //第三个Job
        Job job_3 = Job.getInstance(conf);
        job_3.setMapperClass(CommoutMartrix03.CommoutMartrix03Mapper.class);
        job_3.setReducerClass(CommoutMartrix03.CommoutMartrix03Reducer.class);
        job_3.setOutputKeyClass(Text.class);
        job_3.setOutputValueClass(Text.class);
        TextInputFormat.setInputPaths(job_3,one_2_3);
        TextOutputFormat.setOutputPath(job_3,oneOutput);

        //第四个Job这个可以作为独立的Job
        Job job_4 = Job.getInstance(conf);
        job_4.setMapperClass(BuyMartrix01.BuyMartrix01Mapper.class);
        job_4.setReducerClass(BuyMartrix01.BuyMartrix01Reducer.class);
        job_4.setOutputKeyClass(Text.class);
        job_4.setOutputValueClass(Text.class);
        TextInputFormat.setInputPaths(job_4,input);
        TextOutputFormat.setOutputPath(job_4,twoOutput);

        //第五个Job
        Job job_5 = Job.getInstance(conf);
        job_5.setMapperClass(Recommend01.Recommend01Mapper.class);
        job_5.setReducerClass(Recommend01.Recommend01Reducer.class);
        job_5.setMapOutputKeyClass(Text.class);
        job_5.setMapOutputValueClass(Text.class);
        job_5.setOutputKeyClass(Text.class);
        job_5.setOutputValueClass(IntWritable.class);
        TextInputFormat.setInputPaths(job_5,twoOutput,oneOutput);
        TextOutputFormat.setOutputPath(job_5,three_1);

        //第六个Job
        Job job_6 = Job.getInstance(conf);
        job_6.setMapperClass(Recommend02.Recommend02Mapper.class);
        job_6.setReducerClass(IntSumReducer.class);
        job_6.setOutputKeyClass(Text.class);
        job_6.setOutputValueClass(IntWritable.class);
        TextInputFormat.setInputPaths(job_6,three_1);
        TextOutputFormat.setOutputPath(job_6,threeOutput);

        //第七个Job
        Job job_7 = Job.getInstance(conf);
        job_7.setMapperClass(RecommendUnique.RecommendUniqueMapper.class);
        job_7.setReducerClass(RecommendUnique.RecommendUniqueReducer.class);
        job_7.setOutputKeyClass(Text.class);
        job_7.setOutputValueClass(Text.class);
        TextInputFormat.setInputPaths(job_7,threeOutput,input);
        TextOutputFormat.setOutputPath(job_7,output);

        //配置工作流
        ControlledJob controlledJob_1=
                new ControlledJob(
                        job_1.getConfiguration());
        controlledJob_1.setJob(job_1);

        ControlledJob controlledJob_2=
                new ControlledJob(
                        job_2.getConfiguration());
        controlledJob_2.setJob(job_2);

        ControlledJob controlledJob_3=
                new ControlledJob(
                        job_3.getConfiguration());
        controlledJob_3.setJob(job_3);

        ControlledJob controlledJob_4=
                new ControlledJob(
                        job_4.getConfiguration());
        controlledJob_4.setJob(job_4);

        ControlledJob controlledJob_5=
                new ControlledJob(
                        job_5.getConfiguration());
        controlledJob_5.setJob(job_5);

        ControlledJob controlledJob_6=
                new ControlledJob(
                        job_6.getConfiguration());
        controlledJob_6.setJob(job_6);

        ControlledJob controlledJob_7=
                new ControlledJob(
                        job_7.getConfiguration());
        controlledJob_7.setJob(job_7);

        //配置依赖关系2->1 3->2
        controlledJob_2.addDependingJob(controlledJob_1);
        controlledJob_3.addDependingJob(controlledJob_2);

        //5—>3 5->4
        controlledJob_5.addDependingJob(controlledJob_3);
        controlledJob_5.addDependingJob(controlledJob_4);

        //6->5 7->6
        controlledJob_6.addDependingJob(controlledJob_5);
        controlledJob_7.addDependingJob(controlledJob_6);

        //添加工作流
        JobControl jobControl=
                new JobControl(
                        "Recomend");

        jobControl.addJob(controlledJob_1);
        jobControl.addJob(controlledJob_2);
        jobControl.addJob(controlledJob_3);
        jobControl.addJob(controlledJob_4);
        jobControl.addJob(controlledJob_5);
        jobControl.addJob(controlledJob_6);
        jobControl.addJob(controlledJob_7);

        new Thread(jobControl).start();
        while(true){
            for(ControlledJob cj:jobControl.getRunningJobList()){
                cj.getJob().monitorAndPrintJob();
            }
            if(jobControl.allFinished()) {

                System.out.println("任务完成！");
                return;
            }
        }
    }

}
