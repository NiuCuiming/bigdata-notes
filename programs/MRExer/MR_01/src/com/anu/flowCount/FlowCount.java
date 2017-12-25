package com.anu.flowCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class FlowCount {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowCount.class);
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //设置分区，结果又应该分配在5个文件中，所以应该设置reducer的数量是5;
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(6);



        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置输入输出目录
        FileInputFormat.setInputPaths(job,new Path("C:/FlowCount/input"));
        Path path = new Path("C:/FlowCount/output");
        //获取文件系统，删除存在的输出目录
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,path);

        if (job.waitForCompletion(true)){
            System.out.println("job执行结束！");
        } else {
            System.out.println("job执行失败！");
        }
    }


    static class FlowCountMapper extends Mapper<LongWritable,Text,FlowBean,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //切分
            String line = value.toString();
            String[] fields = line.split("\t");

            //封装
            int length = fields.length;
            FlowBean bean = new FlowBean(Long.parseLong(fields[length-3]),Long.parseLong(fields[length-2]));

            //写出
            context.write(bean,new Text(fields[1]));
        }
    }


    static class FlowCountReducer extends Reducer<FlowBean,Text,Text,FlowBean> {

        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //写出手机号和流量汇总,因为被一个bean都不一样，所以每一个bean对应的value只用一个
            Text phrnum = values.iterator().next();
            context.write(phrnum, key);
        }
    }

    //自定义分区规则按照手机号的归属地分区
    static class ProvincePartitioner extends Partitioner<FlowBean,Text> {

        //定义归属地和手机号的Map
        static HashMap<String,Integer> provinceMap = new HashMap<String,Integer>();
        static {

            provinceMap.put("135",0);
            provinceMap.put("136",1);
            provinceMap.put("137",2);
            provinceMap.put("138",3);
            provinceMap.put("139",4);

            //其它的归属其它省份
        }

        @Override
        public int getPartition(FlowBean flowBean, Text text, int numPartitions) {

            Integer code = provinceMap.get(text.toString().substring(0,3));

            //对应代码在Map存在，就返回Map中的值，否则返回5
            return code == null ? 5:code;


        }
    }

}
