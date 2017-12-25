package com.anu.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/*
    private String id;
    private String date;
    private String amount;
    private String pid;
    private String name;
    private String category_id;
    private String price;

order.txt
    id	    date	    pid	    amount
    1001	20150710	P0001	2

product
    id	    name	category_id price
    P0001	小米5	C01	        2

    注意这种处理方式，把连接操作全部放到reducer端来执行，这样reducer端的负载比较大，
    而且会造成数据倾斜：解释某个reducer所对应的连接条目数比较多，需要很长时间才能连接完成，而另一个连接条目数很少，很快就完成，这样造成
    一些执行任务的主机很清闲，而另一些很紧张！

    解决这种问题的方案是：让map端就完成连接操作，以本例来说，让map端关联小表，即可以将小表分发到所有的map节点，
    这样，map节点就可以在本地对自己所读到的大表数据进行join并输出最终结果，可以大大提高join操作的并发度，加快处理速度



 */
public class OrderJoin {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(OrderJoin.class);
        job.setMapperClass(OrderJoinMapper.class);
        job.setReducerClass(OrderJoinReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(OrderBean.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrderBean.class);

        FileInputFormat.setInputPaths(job,new Path("C:/MR/OrderJoin/input/"));
        Path path = new Path("C:/MR/OrderJoin/output/");
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

    static class OrderJoinMapper extends Mapper<LongWritable,Text,IntWritable,OrderBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //读取行，切分
            String[] fileds = value.toString().split("\t");

            //获取切片属于的文件名，按照文件名分装对象
            FileSplit inputSplit = (FileSplit) context.getInputSplit();     //获取文件的切片对象
            String name = inputSplit.getPath().getName();                   //获取文件名

            IntWritable pid = null;
            if(name.equals("order.txt")){
                pid = new IntWritable(Integer.parseInt(fileds[2]));
                OrderBean orderBean =
                        new OrderBean(fileds[0],fileds[1],fileds[3],fileds[2],"null","null","null");
                context.write(pid,orderBean);

            } else {
                pid = new IntWritable(Integer.parseInt(fileds[0]));
                OrderBean orderBean =
                        new OrderBean("null","null","null",fileds[0],fileds[1],fileds[2],fileds[3]);
                context.write(pid,orderBean);
            }
        }
    }

    static class OrderJoinReducer extends Reducer<IntWritable,OrderBean,NullWritable,OrderBean>{

        @Override
        protected void reduce(IntWritable key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {

            List<OrderBean> orders = new ArrayList<>();
            List<OrderBean> products = new ArrayList<>();

            //分成两类OrderBean,再做联合操作
            for (OrderBean orderBean : values) {

                OrderBean temp = new OrderBean(orderBean);

                if("null".equals(temp.getName())) {

                    orders.add(temp);
                } else {
                    products.add(temp);
                }
            }

            //联合操作输出
            for (OrderBean order : orders) {

                for (OrderBean product : products) {

                    order.setName(product.getName());
                    order.setCategory_id(product.getCategory_id());
                    order.setPrice(product.getPrice());
                    context.write(NullWritable.get(),order);

                }
            }


        }
    }
}
