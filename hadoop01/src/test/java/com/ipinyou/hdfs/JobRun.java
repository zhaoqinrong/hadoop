package com.ipinyou.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import sun.applet.Main;

import java.io.IOException;

public class JobRun {
    public static void main(String[] args) throws IOException {
        Configuration conf =new Configuration();
        conf.set("mapred.job.tracker","node07:9001");
        Job job = new Job(conf);
        job.setJarByClass(JobRun.class);
        job.setMapperClass(MapReduceDemo.class);
        job.setReducerClass(WcReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);//设置reduce任务的个数
//        FileInputFormat.addInputPath(job,new Path("/hi/input.txt"));

    }
}
