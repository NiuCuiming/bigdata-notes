package com.anu.exer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

public class WordCount1 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        //把业务逻辑相关的信息（哪个是mapper，哪个是reducer，要处理的数据在哪里，输出的结果放哪里。。。。。。）描述成一个job对象      //把这个描述好的job        //

        //job.setOutputKeyClass和job.setOutputValueClas在默认情况下是同时设置map阶段和reduce阶段的输出，也就是说只有map和reduce输出是一样的时候才不会出问题。
        //当map和reduce输出是不一样的时候就需要通过job.setMapOutputKeyClass和job.setMapOutputValueClas来设置map阶段的输出。


        //获取配置文件，通过配置文件构造job对象
        Configuration configuration = new Configuration();


        //显示设定一些参数，下面的设置也是默认的设置,这样可以在本地模拟mr运行
//        configuration.set("fs.defaultFS", "file://localhost");
//        configuration.set("mapreduce.framework.name", "local");

        //显示设定一些参数，调用hdfs的文件,在本地执行
        //configuration.set("fs.deFaultFS","hdfs://node6:9000");
        //configuration.set("yarn.resourcemanager.hostname","local");

        Job job = Job.getInstance(configuration);

        //指定要执行任务的类
        job.setJarByClass(WordCount1.class);

        //设置job的mapper和reducer
        job.setMapperClass(WCMapper.class);
        job.setCombinerClass(WCReducer.class);
        job.setReducerClass(WCReducer.class);
        job.setInputFormatClass(WholeFileInputFormat.class);

        //设置Map输出的类型，错误点！！！
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputKeyClass(IntWritable.class);

//      设置任务输出的kv类型(reducer的输出类型)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //执行处理数据所在位置
        FileInputFormat.setInputPaths(job,new Path("C:/weblog/input"));
        Path outputPath = new Path("C:/weblog/output1");

//        FileInputFormat.setInputPaths(job,new Path("hdfs://node6:9000/wc/input"));
//        Path outputPath = new Path("hdfs://node6:9000/wc/output/");

        //获取文件系统,这种方法可以删除成功
//        FileSystem fs =FileSystem.get(new URI("hdfs://node6:9000"), configuration, "root");
        FileSystem fs =FileSystem.get(configuration);
        //这种方法删除不成功
//        FileSystem fs =FileSystem.get(configuration);
        //如果输出目录存在，自动删除
        if(fs.exists(outputPath)) {
            fs.delete(outputPath,true);
        }
        FileOutputFormat.setOutputPath(job,outputPath);

        //提交任务
        boolean b = job.waitForCompletion(true);
        if(b) {
            System.out.println("job执行成功!");
        }

    }




    static class WCMapper extends Mapper<NullWritable,Text,Text,IntWritable> {

        @Override
        protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {

            /*
            获取文件的每一行，按照空格切分
             */
            String line = value.toString();
            String[] words = line.split(" ");

            /*
            把每一个word当作key，1当作值，写出
             */
            for (String word:words) {

                Text text = new Text(word);
                IntWritable intWritable = new IntWritable(1);
                context.write(text, intWritable);
            }
        }


    }

    static class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            /*获取每个单词的values，
            对每个单词的values求和*/

            int counts = 0;
            for (IntWritable count: values) {
                counts += count.get();
            }
            /*写出key和求和值*/
            context.write(key,new IntWritable(counts));

        }
    }



    //自定义InputFormat
    static class WholeFileInputFormat extends FileInputFormat<NullWritable, Text>{

        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
        }

        @Override
        public RecordReader<NullWritable, Text> createRecordReader(
                InputSplit split, TaskAttemptContext context) throws IOException,
                InterruptedException {
            WholeFileRecordReader reader = new WholeFileRecordReader();
            reader.initialize(split, context);
            return reader;
        }

    }


    /*
    RecordReader的核心工作逻辑：
    通过nextKeyValue()方法去读取数据构造将返回的key   value
    通过getCurrentKey 和 getCurrentValue来返回上面构造好的key和value
     */
    //自定义Recoder
    static class WholeFileRecordReader extends RecordReader<NullWritable, Text> {

        private FileSplit fileSplit;
        private Configuration conf;
        //private BytesWritable value = new BytesWritable();
        private Text text;
        private boolean processed = false;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            this.fileSplit = (FileSplit) split;
            this.conf = context.getConfiguration();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {

            //这个方法用来把切片合并
            if (!processed) {
                byte[] contents = new byte[(int) fileSplit.getLength()];
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                FSDataInputStream in = null;
                try {
                    in = fs.open(file);
                    IOUtils.readFully(in, contents, 0, contents.length);
                    //value.set(contents, 0, contents.length);
                    text = new Text(contents);
                } finally {
                    IOUtils.closeStream(in);
                }
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException,
                InterruptedException {
            return NullWritable.get();
        }

        @Override
        public Text getCurrentValue() throws IOException,
                InterruptedException {
            return text;
        }

        /**
         * 返回当前进度
         */
        @Override
        public float getProgress() throws IOException {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
    }

}
