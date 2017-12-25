package com.anu.Sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;

import java.io.IOException;

public class SequenceFileTest {

    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        Path outPath = new Path("C:/MR/sequence/file/textSeq");

        //定义序列化文件的key，value
        IntWritable key = new IntWritable();
        Text value = new Text();

        /*
        获取SequenceFile的写入器对象
         */
        //写出路径
        Writer.Option op1 = Writer.file(outPath);
        //序列文件的key类型，value类型
        Writer.Option op2 = Writer.keyClass(key.getClass());
        Writer.Option op3 = Writer.valueClass(value.getClass());

        //设置压缩类型：块级别的压缩，记录级别的压缩
        Writer.Option op4 = Writer.compression(SequenceFile.CompressionType.RECORD, new BZip2Codec());
        //Writer writer = SequenceFile.createWriter(conf,op1,op2,op3,op4);

        // 获取文件写入器
        Writer writer = SequenceFile.createWriter(conf,op1,op2,op3);

        for (int i = 0; i < 100000; i++) {

            //同步标记块
            if(i%3 == 0) {
                writer.sync();
            }
            key.set(i);
            value.set(DATA[i%DATA.length]);
            writer.append(key,value);
        }

        writer.close();
    }




}
