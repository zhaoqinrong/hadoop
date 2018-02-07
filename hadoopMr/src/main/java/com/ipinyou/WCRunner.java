package com.ipinyou;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 用来描述一个特定的作业
 * 比如该作业使用哪个类作为逻辑处理中的map,哪个作为reduce
 * 还可以指定该作业要处理的数据所在的路径
 * 还可以指定作业输出的结果放到哪个路径
 */
public class WCRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        //设置整个job所用的类在哪个jar包
        job.setJarByClass(WCRunner.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCreduce.class);
        //指定reduce的输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //指定mapper的输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //指定原始数据在哪来
        FileInputFormat.setInputPaths(job,new Path("/wc/input/"));
        FileOutputFormat.setOutputPath(job,new Path("/wc/output/"));
        job.waitForCompletion(true);
    }
}
