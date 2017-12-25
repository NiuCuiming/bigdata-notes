package com.anu.join.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextTuple implements WritableComparable<TextTuple>{

    private Text aitistId = new Text();
    private Text tag = new Text();

    public TextTuple() {
    }

    public TextTuple(Text aitistId, Text tag) {
        this.aitistId = aitistId;
        this.tag = tag;
    }

    public Text getAitistId() {
        return aitistId;
    }

    public void setAitistId(Text aitistId) {
        this.aitistId = aitistId;
    }

    public Text getTag() {
        return tag;
    }

    public void setTag(Text tag) {
        this.tag = tag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        aitistId.write(dataOutput);
        tag.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        aitistId.readFields(dataInput);
        tag.readFields(dataInput);
    }

    @Override
    public int compareTo(TextTuple o) {

        int result = getAitistId().compareTo(o.getAitistId());

        return result == 0 ? getTag().compareTo(o.getTag()) : result;
    }

    @Override
    public String toString() {
        return  aitistId + "\t" + tag;
    }
}
