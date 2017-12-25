package com.anu.join.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class JoinRecordWithArtistId {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //因为这个mr中有两个Mapper，并且需要单独设置输入文件
        MultipleInputs.addInputPath(job,new Path("C:/MR/artist/input/artist.txt"),
                TextInputFormat.class,JoinArtistMapper.class);

        MultipleInputs.addInputPath(job,new Path("C:/MR/artist/input/user_artist.txt"),
                TextInputFormat.class,JoinRecordMapper.class);

//        job.setNumReduceTasks(4);

        /*
        设置分区器和分组比较器
         */
        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(keyGroupComparator.class);

        /*
        设置reduceclass
         */
        job.setReducerClass(JoinReducer.class);


        //设置输出路径
        Path outpath =new Path("C:/MR/artist/output_reduce");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outpath)){
            fs.delete(outpath, true);
        }
        TextOutputFormat.setOutputPath(job,outpath);

        //设置输出格式
        job.setMapOutputKeyClass(TextTuple.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        boolean f= job.waitForCompletion(true);

        if(f){
            System.out.println("job任务结束");
        }else {
            System.out.println("执行失败！");
        }


    }

    static class JoinArtistMapper extends Mapper<LongWritable,Text,TextTuple,Text> {

        ArtistMetaParser parser = new ArtistMetaParser();//唱片解析器
        TextTuple k = new TextTuple();//联合键,ID,Tag
        Text v = new Text();//value v 唱片名和日期

        /*
        实现：获取唱片数据实现TextTuple的封装输出
        key:唱片ID
        value：唱片名字和发行日期
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            parser.parser(value.toString());
            if(parser.isValid()) {
                k.setAitistId(new Text(parser.getArtistId()));
                k.setTag(new Text("00"));
                v.set(parser.getArtistName()+"\t"+parser.getDate());
                context.write(k,v);
            }
        }
    }

    static class JoinRecordMapper extends Mapper<LongWritable,Text,TextTuple,Text> {


        ArtistRecordParser parser = new ArtistRecordParser();//唱片解析器
        TextTuple k = new TextTuple();//联合键,ID,Tag
        Text v = new Text();//value v 唱片名和日期

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            parser.parser(value.toString());
            if(parser.isValid()) {
                k.setAitistId(new Text(parser.getArtistId()));
                k.setTag(new Text("11"));
                v.set(parser.getDate()+"\t"+parser.getCount());
                context.write(k,v);
            }
        }
    }

    static class JoinReducer extends Reducer<TextTuple,Text,Text,Text> {

        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void reduce(TextTuple key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //把联合好的数据输出
            Iterator<Text> it = values.iterator();

            //获取唱片ID
            k = new Text(key.getAitistId()+">>>"+key.getTag());

            //获取唱片记录,对应value集合中的第一个元素
            Text artistInfo = new Text(it.next()); /****错误的地方！！！不能直接Text artistInfo = it.next()*******/
            while (it.hasNext()) {                  /*迭代器每次通过it.next()获取的对象都存放在相同的堆区，所以不能直接*/
                Text recodInfo = it.next();
                v.set(artistInfo+"\t"+recodInfo);
                context.write(k,v);
            }
        }
    }
}


/*
分区器：只受ID影响分区
 */
class KeyPartitioner extends Partitioner<TextTuple,Text> {

    @Override
    public int getPartition(TextTuple textTuple, Text text, int numPartitions) {
        return (textTuple.getAitistId().hashCode()*127)%numPartitions;
    }
}

/*
分组比较器
 */
class keyGroupComparator extends WritableComparator {

    public keyGroupComparator() {
        super(TextTuple.class,true);   //第二个参数！！！！！！！！！！
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        TextTuple o1 = (TextTuple)a;
        TextTuple o2 = (TextTuple)b;

        return o1.getAitistId().compareTo(o2.getAitistId());

    }
}
