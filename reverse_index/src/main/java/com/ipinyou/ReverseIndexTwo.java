package com.ipinyou;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.Iterator;

public class ReverseIndexTwo {
    public static class ReverseIndexTwoMapper extends Mapper<LongWritable,Text,Text,Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");
            String[] wordAndfileName = StringUtils.split(fields[0], "-->");
            context.write(new Text(wordAndfileName[0]),new Text(wordAndfileName[1]+":"+Long.parseLong(fields[1])));
        }
    }
    public static class ReverseIndexTwoReducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            String a="";
            while (iterator.hasNext()){
                Text next = iterator.next();
                a+=next;
            }
            context.write(key,new Text(a));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        //设置整个job所用的类在哪个jar包
        job.setJarByClass(ReverseIndexTwo.class);
        job.setMapperClass(ReverseIndexTwoMapper.class);
        job.setReducerClass(ReverseIndexTwoReducer.class);
        //指定reduce的输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //指定mapper的输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //指定原始数据在哪来
        Path path = new Path(args[1]);
        FileSystem fileSystem=FileSystem.get(conf);
        if (fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
    }
}
