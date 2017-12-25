package com.anu.dbInputOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/*
调试过一直失败，原因是解析器解析的文件有问题！！！！！！！！！！！！！！
 */
public class HdfsToDb {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        TextInputFormat.addInputPath(job, new Path("C:/MR/Weather/input/999999-99999-1990"));

        DBConfiguration.configureDB(
                job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://172.16.0.100:3306/hadoop",
                "hadoop", "hadoop");

        //相当于设置insert into station_tbl(year,station,temperature) values(?, ?, ?)
        DBOutputFormat.setOutput(job, "tbl_anu",
                "year", "station", "temperature");

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setMapOutputKeyClass(YearStation.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(YearStationDB.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        //执行Job
        if (job.waitForCompletion(true)){
            System.out.println("job执行结束！");
        } else {
            System.out.println("job执行失败！");
        }
    }

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text,YearStation, IntWritable> {

        private NcdcRecordParser parser =
                new NcdcRecordParser();
        private YearStation k = new YearStation();
        private IntWritable v = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                k.set(parser.getYear(), parser.getStationId());
                v.set(parser.getAirTemperature());
                context.write(k, v);
            }
        }
    }

    static class MaxTemperatureReducer
            extends Reducer<YearStation, IntWritable,
                        YearStationDB, NullWritable> {
        private YearStationDB k = new YearStationDB();

        public void reduce(YearStation key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            k.setYear(key.getYear().get());
            k.setStationId(key.getStationId().toString());
            k.setTemperature(maxValue);
            context.write(k, NullWritable.get());
        }
    }
}


class YearStation implements WritableComparable<YearStation> {

    private IntWritable year;
    private Text stationId;

    public YearStation(){
        year = new IntWritable();
        stationId = new Text();
    }

    public YearStation(YearStation ys){
        year = new IntWritable();
        year.set(ys.year.get());
        stationId = new Text(ys.stationId);
    }

    @Override
    public void readFields(DataInput input)
            throws IOException{
        year.readFields(input);
        stationId.readFields(input);
    }
    @Override
    public void write(DataOutput output)throws IOException {
        year.write(output);
        stationId.write(output);
    }
    @Override
    public int compareTo(YearStation ys){
        return (year.compareTo(ys.year) != 0)?
                (year.compareTo(ys.year))
                :(stationId.compareTo(ys.stationId));
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof YearStation)){
            return false;
        }
        YearStation  ys = (YearStation)obj;
        return (year == ys.year) && (stationId.equals(ys.stationId));
    }
    @Override
    public int hashCode(){
        return Math.abs(year.hashCode()*127 + stationId.hashCode());
    }
    @Override
    public String toString(){
        return year + "\t" + stationId;
    }

    public void set(IntWritable year, Text stationId){
        set(year.get(), stationId.toString());
    }
    public void set(int year, String stationId){
        this.year.set(year);
        this.stationId.set(stationId);
    }
    public IntWritable getYear(){return year;}

    public Text getStationId(){return stationId;}

}


/*
气象数据解析器
 */
class NcdcRecordParser{

    private static final int MISSING = 9999;
    private String	stationId;
    private int		year;
    private int		temperature;
    private boolean isValidTemperature;

    public NcdcRecordParser(){}

    public void parse(String line){
        if(line.length() < 93){
            isValidTemperature = false;
            return;
        }
        stationId = line.substring(0, 15);
        year = Integer.parseInt(line.substring(15, 19));
        if(line.charAt(87) == '+'){
            temperature = Integer.parseInt(
                    (line.substring(88, 92)));
        }else{
            temperature = Integer.parseInt(
                    (line.substring(87, 92)));
        }
        String quality = line.substring(92, 93);
        this.isValidTemperature =
                (temperature != MISSING) &&
                        (quality.matches("[01459]"));

    }
    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public void parse(Text value){
        parse(value.toString());
    }

    public int getYear(){return year;}

    public void setYear(int year){this.year = year;}

    public int getAirTemperature(){return temperature;}

    public void setAirTemperature(int temperature){
        this.temperature = temperature;
    }

    public void setValidTemperature(boolean isValidTemperature) {
        this.isValidTemperature = isValidTemperature;
    }
    public boolean isValidTemperature(){return isValidTemperature;}
}
