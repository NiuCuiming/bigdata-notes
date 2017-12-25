package com.anu.Sequence;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SmallFileToSeqFile extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SmallFileToSeqFile(), args));
    }


    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        //设置InputFormat,这个需要读取文件按切片的全部数据，所以需要自定义
        // outputForMat
        job.setInputFormatClass(WholeInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //设置路径
        WholeInputFormat.addInputPath(job, new Path("C:/MR/sequence/file/"));
        SequenceFileOutputFormat.setOutputPath(job, new Path("C:/MR/sequence/output"));

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(SequenceFileMapper.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

        private Text fileKey;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            //通过切片获取文件名
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            fileKey = new Text(inputSplit.getPath().getName());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

            //文件名为key，文件内容字节码为value
            context.write(fileKey,value);
        }
    }

}

class WholeInputFormat extends FileInputFormat<NullWritable, BytesWritable> {


    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        WholeRecordReader wrr = new WholeRecordReader();
        wrr.initialize(split, context);
        return wrr;

    }
}

class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private FileSplit fileSplit;
    private Configuration conf;
    private BytesWritable value = new BytesWritable();
    private boolean processed = false;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            byte[] contents = new byte[(int)fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try{
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);
                value.set(contents, 0, contents.length);
            }finally{
                IOUtils.closeQuietly(in);
            }
            processed = true;
        }
        return processed;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        fileSplit.getPath().getFileSystem(conf).close();
    }

}
