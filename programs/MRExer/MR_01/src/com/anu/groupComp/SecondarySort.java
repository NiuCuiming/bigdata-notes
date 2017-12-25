package com.anu.groupComp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SecondarySort {

    /*
    测试失败！！
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SecondarySort.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);


        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("C:/MR/SecondarySort/input/"));
        FileOutputFormat.setOutputPath(job, new Path("C:/MR/SecondarySort/output/"));


        //指定shuffle所使用的GroupingComparator类
        job.setGroupingComparatorClass(ItemidGroupingComparator.class);

        job.waitForCompletion(true);

    }

    static class SecondarySortMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split("\t");
            context.write(new OrderBean(fields[0],fields[1],fields[2]),NullWritable.get());
        }
    }

    static class SecondarySortReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable> {
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(key,NullWritable.get());
        }
    }


    //自定义分组器，reduce之前的merger排序操作。
    static class ItemidGroupingComparator extends WritableComparator {

        protected ItemidGroupingComparator() {

            super(OrderBean.class, true);
        }

        public int compare(WritableComparable a, WritableComparable b) {

            OrderBean abean = (OrderBean) a;
            OrderBean bbean = (OrderBean) b;

            //将ProduceId相同的bean都视为相同，从而聚合为一组
            return abean.getProduceId().compareTo(bbean.getProduceId());
        }

    }

}
