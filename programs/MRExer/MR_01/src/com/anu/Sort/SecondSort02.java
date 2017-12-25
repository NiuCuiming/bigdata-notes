package com.anu.Sort;

import com.anu.weather.Weather_exer01;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class SecondSort02 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(Weather_exer01.class);

        //设置Mapper和Reducer
        job.setMapperClass(SecondSort02Mapper.class);
        job.setReducerClass(SecondSort02Reducer.class);

        //Map和Reduce输出格式
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置自定义的partition和GroupComparator
        job.setPartitionerClass(SecondPartitioner.class);
        job.setGroupingComparatorClass(SecondGroupComparator.class);

        //设置文件源和目的
        FileInputFormat.addInputPath(job, new Path("C:/MR/Sort/input"));
        Path outpath =new Path("C:/MR/Sort/output_02");

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

    static class SecondSort02Mapper extends Mapper<LongWritable,Text,PairWritable,IntWritable> {

        private PairWritable mapOutKey = new PairWritable();
        private IntWritable mapOutValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String lineValue = value.toString();
            String[] strs = lineValue.split(" ");

            //设置组合key和value ==> <(key,value),value>
            mapOutKey.set(strs[0], Integer.valueOf(strs[1]));
            mapOutValue.set(Integer.valueOf(strs[1]));

            context.write(mapOutKey, mapOutValue);
        }
    }

    static class SecondSort02Reducer extends Reducer<PairWritable,IntWritable,Text,IntWritable> {

        private Text outPutKey = new Text();

        @Override
        protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            for(IntWritable value : values) {
                outPutKey.set(key.getFirst());
                context.write(outPutKey, value);
            }
        }
    }
}

//自定义分组比较器
class SecondGroupComparator implements RawComparator<PairWritable> {

    @Override
    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
        return WritableComparator.compareBytes(bytes, 0, i1-4, bytes1, 0, i3-4);
    }

    /*
    * 对象比较
    */
    @Override
    public int compare(PairWritable o1, PairWritable o2) {
        return o1.getFirst().compareTo(o2.getFirst());
    }
}

//自定义分区规则
class SecondPartitioner extends org.apache.hadoop.mapreduce.Partitioner<PairWritable,IntWritable> {

    @Override
    public int getPartition(PairWritable key, IntWritable value, int numPartitions) {

        return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}


//自定义数据类型
class PairWritable implements WritableComparable<PairWritable> {


    private String first;
    private int second;

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public PairWritable() {
    }

    public PairWritable(String first, int second) {
        this.first = first;
        this.second = second;
    }

    public void set(String first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(PairWritable o) {

        /*
        先比较第一个字符，在比较第二个字符
         */
        int result = this.first.compareTo(o.getFirst());

        if(result == 0) {
            return result;
        } else {
            return Integer.valueOf(this.getSecond()).compareTo(Integer.valueOf(o.getSecond()));
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeInt(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readUTF();
        this.second = dataInput.readInt();
    }
}
