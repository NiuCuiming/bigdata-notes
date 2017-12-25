package com.anu.TotalPartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class TotalPartiton {

    public static void main(String[] args) throws IOException {

        Configuration con = new Configuration();

        Job job = Job.getInstance(con);

        job.setNumReduceTasks(5);

    }

}
