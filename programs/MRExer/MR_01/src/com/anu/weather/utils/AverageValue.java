package com.anu.weather.utils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AverageValue implements Writable {

    private int num;
    private double avg;

    public AverageValue() {

    }

    public AverageValue(int num, double avg) {
        this.num = num;
        this.avg = avg;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    @Override
    public String toString() {
        return "AverageValue{" +
                "num=" + num +
                ", avg=" + avg +
                '}'+System.lineSeparator();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(num);
        dataOutput.writeDouble(avg);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        num = dataInput.readInt();
        avg = dataInput.readDouble();

    }
}
