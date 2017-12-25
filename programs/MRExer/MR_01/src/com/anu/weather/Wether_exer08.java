package com.anu.weather;


import com.anu.weather.utils.WeatherRecordParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现年份和温度的二级排序
 */
public class Wether_exer08 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setNumReduceTasks(2);

        //设置自定义分区器，默认按照Hash取模
        job.setPartitionerClass(MyPartitoner.class);
        //实现分组比较器，默认按照key的原生比较方式。
        job.setGroupingComparatorClass(MyComparator.class);


        //设置Mapper和Reducer
        job.setMapperClass(Wether_exer08Mapper.class);
        job.setReducerClass(Weather_exer08Reducer.class);

        //Map和Reduce输出格式
        job.setOutputKeyClass(YearTem.class);
        job.setOutputValueClass(Text.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/Weather/input/small.txt"));
        Path outpath =new Path("C:/MR/Weather/output_09");

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

    static class Wether_exer08Mapper extends Mapper<LongWritable,Text,YearTem,Text> {

        WeatherRecordParser parser = new WeatherRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            parser.parser(value.toString());
            IntWritable year = new IntWritable(parser.getYear());
            IntWritable temp = new IntWritable(parser.getTemperature());
            context.write(new YearTem(year,temp),new Text(parser.getStationId()));
        }
    }

    static class Weather_exer08Reducer extends Reducer<YearTem,Text,YearTem,Text> {

        @Override
        protected void reduce(YearTem key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text tem: values) {
                context.write(key,tem);
            }
        }
    }
}

/*
自定义排序器
 */

/*
自定义分组比较器
 */
class MyComparator extends WritableComparator {

    public MyComparator(){

        super(YearTem.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        YearTem yt1 = (YearTem)a;
        YearTem yt2 = (YearTem)b;

        return yt1.getYear().compareTo(yt2.getYear());

    }
}


/*
分区器,保持之前的分区不变
 */
class MyPartitoner extends Partitioner<YearTem, Text> {

    @Override
    public int getPartition(YearTem yearTem, Text text, int numPartitions) {
        return (yearTem.getYear().hashCode()*127)%numPartitions;
    }
}

/*
联合主键类
 */
class YearTem implements WritableComparable<YearTem> {

    private IntWritable year = new IntWritable();
    private IntWritable temp = new IntWritable();

    public YearTem() {
    }

    public YearTem(IntWritable year, IntWritable temp) {
        this.year = year;
        this.temp = temp;
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year = year;
    }

    public IntWritable getTemp() {
        return temp;
    }

    public void setTemp(IntWritable temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return year + "\t" + temp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        year.write(dataOutput);
        temp.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        year.readFields(dataInput);
        temp.readFields(dataInput);

    }

    @Override
    public int compareTo(YearTem o) {

        int temp = getYear().compareTo(o.getYear());
        return temp == 0 ? getTemp().compareTo(o.getTemp()):temp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        YearTem yearTem = (YearTem) o;

        if (year != null ? !year.equals(yearTem.year) : yearTem.year != null) return false;
        return temp != null ? temp.equals(yearTem.temp) : yearTem.temp == null;
    }

    @Override
    public int hashCode() {
        int result = year != null ? year.hashCode() : 0;
        result = 31 * result + (temp != null ? temp.hashCode() : 0);
        return result;
    }
}
