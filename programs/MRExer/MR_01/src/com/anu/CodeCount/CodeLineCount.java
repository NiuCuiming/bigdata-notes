package com.anu.CodeCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CodeLineCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        FileStatus[] fileStatuses = fs.listStatus(new Path("D:\\\\software_java\\\\java_ide\\\\IdeaProjects\\\\MRExer\\\\MR_01\\\\src\\\\com\\anu"));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fileStatuses.length; i++) {
            sb.append(fileStatuses[i].getPath()+",");
            if(i == fileStatuses.length -1 ){
                sb.append(fileStatuses[i].getPath());
            }
        }

        System.out.println(sb.toString());
        Job job = Job.getInstance(conf);

        job.setMapperClass(WcCountMapper.class);

        TextInputFormat.addInputPaths(job,"file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/block,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/CodeCount,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/Compress,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/cooccurence,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/crud,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/dbInputOutputFormat,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/exer,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/findFriends,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/flowCount,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/groupComp,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/inverse_index,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/patentReference,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/search,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/Sequence,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/Sort,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/TotalPartitioner,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/wc,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/workFlow,file:/D:/software_java/java_ide/IdeaProjects/MRExer/MR_01/src/com/anu/workFlow");
        TextOutputFormat.setOutputPath(job,new Path("D:\\software_java\\java_ide\\IdeaProjects\\MRExer\\MR_01\\src\\com\\SUMOUT"));

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }

    static class WcCountMapper extends Mapper<LongWritable, Text,Text,IntWritable> {

        private IntWritable one=new IntWritable(1);
        private static int lineSum = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            lineSum++;
            context.write(new Text("----->"),new IntWritable(lineSum));

        }
    }
}

