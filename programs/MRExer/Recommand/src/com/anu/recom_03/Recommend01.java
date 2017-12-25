package com.anu.recom_03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;

public class Recommend01 {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setMapperClass(Recommend01Mapper.class);
        job.setReducerClass(Recommend01Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        TextInputFormat.setInputPaths(job,new Path("C:/MR/Recommand/output_wupin_comout03/"),
                new Path("C:/MR/Recommand/output_buy_comout/"));

        Path outpath = new Path("C:/MR/Recommand/output_command_comout/");
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outpath)) {
            fileSystem.delete(outpath,true);
        }
        TextOutputFormat.setOutputPath(job,outpath);

        if (job.waitForCompletion(true)){
            System.out.println("job执行结束！");
        } else {
            System.out.println("job执行失败！");
        }
    }


    public static class Recommend01Mapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            if(split.length == 2) {
                context.write(new Text(split[0]),new Text(split[1]));
            }
        }
    }

    public static class Recommend01Reducer extends Reducer<Text,Text,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String userList = "";
            String goodList = "";

            for (Text text:values) {

                if(text.toString().charAt(1) == '1') {
                    userList = text.toString();
                } else {
                    goodList = text.toString();
                }
            }

            //做计算
            String[] users = userList.split(" ");
            String[] goods = goodList.split(" ");

            //变量
            String user = "";
            String good = "";
            int userBuys = 0;
            int goodsSimlar = 0;

            int commandRate = 0;

            for (int i = 0; i < users.length -1; i++) {

                if(i == 0) {
                    String[] user_buys = users[0].substring(1).split(":");
                    user = user_buys[0];
                    userBuys = Integer.parseInt(user_buys[1]);
                }else {
                    user = users[i].split(":")[0];
                    userBuys = Integer.parseInt(users[i].split(":")[1]);
                }

                for (int j = 0; j < goods.length-1; j++) {

                    if(j == 0) {
                        String[] goods_simlar = goods[0].substring(1).split(":");
                        good = goods_simlar[0];
                        goodsSimlar = Integer.parseInt(goods_simlar[1]);
                    }else{
                        good = goods[j].split(":")[0];
                        goodsSimlar = Integer.parseInt(goods[j].split(":")[1]);
                    }
                    commandRate = userBuys * goodsSimlar;
                    context.write(new Text(user+","+good),new IntWritable(commandRate));
                }
            }
            
        }
    }
}
