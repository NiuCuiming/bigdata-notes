package com.anu.patentReference;

import org.apache.hadoop.io.Text;

public class PatentParaer {

    /*
    数据格式eg:
    1,2
    1,3
    2,3
     */
    private String patentID;    //专利ID
    private String refPatentID; //被引用的专利ID
    private boolean valid;      //行有效性

    public String getPatentID() {
        return patentID;
    }

    public void setPatentID(String patentID) {
        this.patentID = patentID;
    }

    public String getRefPatentID() {
        return refPatentID;
    }

    public void setRefPatentID(String refPatentID) {
        this.refPatentID = refPatentID;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public void parse(String line) {

        String[] strs = line.split(",");

        //合法性判断
        if(strs.length == 2) {
            patentID = strs[0].trim();
            refPatentID = strs[1].trim();

            if(patentID.length()>0&&refPatentID.length()>0) {
                valid = true;
            }
        }
    }

    public void parse(Text line) {
        parse(line.toString());
    }
}
