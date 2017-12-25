package com.anu.flowCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SequenceFileWriterDemo {

    private static final String[] DATA={"One, two, buckle my shoe","Three, four, shut the door","Five, six, pick up sticks",
            "Seven, eight, lay them straight","Nine, ten, a big fat hen"};

    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        String uri="hdfs://node6:9000/exer/numbers.seq";
        Path path=new Path(uri);
        IntWritable key=new IntWritable();
        Text value=new Text();
        //获取SequenceFile的写入对象
        SequenceFile.Writer writer=SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(key.getClass()),
                SequenceFile.Writer.valueClass(value.getClass()));
        for(int i=0;i<100;i++){
            key.set(100-i);
            value.set(DATA[i%DATA.length]);
            System.out.printf("[%s]\t%s\t%s\n",writer.getLength(),key,value);
            //写入数据
            writer.append(key, value);
        }
        writer.close();
    }
}
