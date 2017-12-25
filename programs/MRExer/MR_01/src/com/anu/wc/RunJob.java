package com.anu.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {

	public static void main(String[] args) throws Exception {
		//本地运行模式
			/*
			 * 写一个程序，不要带集群的配置文件（本质是你的mr程序的conf中是否有
			 * mapreduce.framework.name=local以及yarn.resourcemanager.hostname参数）
			 */
		Configuration config =new Configuration();
//			config.set("fs.defaultFS", "hdfs://node1:8020");	//指定操作的文件系统
//			config.set("yarn.resourcemanager.hostname", "node1");
			
			config.set("fs.defaultFS", "file://localhost");
			config.set("mapreduce.framework.name", "local");
			
			try {
				
				FileSystem fs =FileSystem.get(config);
				Job job =Job.getInstance(config);
				job.setJarByClass(RunJob.class);
				
				job.setJobName("wc");
				job.setMapperClass(WcCountMapper.class);
				job.setReducerClass(WcCountReducer.class);
				
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				

				FileInputFormat.addInputPath(job, new Path("D:/wc/input"));
				Path outpath =new Path("D:/wc/output");
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
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
}
