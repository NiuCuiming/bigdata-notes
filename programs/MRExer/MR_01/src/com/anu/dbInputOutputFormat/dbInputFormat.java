package com.anu.dbInputOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class dbInputFormat {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //设置Mapper
        job.setMapperClass(DBInputMapper.class);

        /*
        设置InputFormat为DBInputFormat，这个格式化输入器需要提前配置一些变量和导入Jar包
         */
        //配置数据库相关信息
        DBConfiguration.configureDB(
                job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://172.16.0.100:3306/hadoop",
                "hadoop", "hadoop"
        );

        //配置DBInputFormat
        DBInputFormat.setInput(job,YearStationDB.class,
                "station_tbl",
                "year > 2000",
                "temperature",
                "year", "station","temperature");
        job.setInputFormatClass(DBInputFormat.class);

        //设置输出路径
        Path path = new Path("C:/MR/DB/output");
        //获取文件系统，删除存在的输出目录
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,path);


        // 设置输出类
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        //执行Job
        if (job.waitForCompletion(true)){
            System.out.println("job执行结束！");
        } else {
            System.out.println("job执行失败！");
        }
    }

    static class DBInputMapper extends Mapper<LongWritable, YearStationDB, LongWritable, Text> {
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, YearStationDB value, Context context) throws IOException, InterruptedException {
            outValue.set(value.toString());
            context.write(key, outValue);
        }
    }

}

/*
Mapper的输入value类型必须是DBWritable
 */
class YearStationDB implements DBWritable,WritableComparable<YearStationDB> {

    private int 		year;
    private String 	stationId;
    private int		temperature;

    public YearStationDB(){
    }

    public YearStationDB(int year, String stationId,
                         int temperature){
        this.year = year;
        this.stationId = stationId;
        this.temperature = temperature;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    @Override
    public int compareTo(YearStationDB ys) {
        return (year != ys.year)?
                ((year > ys.year)?1:-1)
                :(stationId.compareTo(ys.stationId));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeUTF(stationId);
        dataOutput.writeInt(temperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        year = dataInput.readInt();
        stationId = dataInput.readUTF();
        temperature = dataInput.readInt();
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {

        ps.setInt(1, year);
        ps.setString(2, stationId);
        ps.setInt(3, temperature);

    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        if(rs == null)return;
        year = rs.getInt(1);
        stationId = rs.getString(2);
        temperature = rs.getInt(3);
    }

    @Override
    public String toString() {
        return getYear() + "," + getStationId() + "," + this.temperature;
    }
}