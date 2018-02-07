package com.ipinyou.hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapReduceDemo extends Mapper<LongWritable,Text,Text,IntWritable>{
    //每次调用map方法会传入split中的一行数据;key该行数据所在文件中的位置下标,value,这一行数据
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();//一行的内容
        StringTokenizer tokenizer = new StringTokenizer(line); //按照空格切这行的数据
        while (tokenizer.hasMoreTokens()){
            String tooken = tokenizer.nextToken();
            context.write(new Text(tooken),new IntWritable(1));//map输出
        }
    }

}
