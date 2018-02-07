package com.ipinyou;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Iterator;

public class ReverseIndexOne {
    public static class StepOneMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1.拿到一行数据
            String line = value.toString();
            //2.切分
            String[] fields = StringUtils.split(line, ' ');
           //获取这一行的文件切片,从文件切片中获取文件名
            FileSplit split = (FileSplit) context.getInputSplit();
            Path path = split.getPath();
            String name = path.getName();
            for (String field:fields){
                context.write(new Text(field+"-->"+name),new LongWritable(1));
            }

        }
    }
    public static class StepOneReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<LongWritable> iterator = values.iterator();
            long counter=0;
            while (iterator.hasNext()){
               counter+= iterator.next().get();
            }
            context.write(key,new LongWritable(counter));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        //设置整个job所用的类在哪个jar包
        job.setJarByClass(ReverseIndexOne.class);
        job.setMapperClass(StepOneMapper.class);
        job.setReducerClass(StepOneReducer.class);
        //指定reduce的输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //指定mapper的输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
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
