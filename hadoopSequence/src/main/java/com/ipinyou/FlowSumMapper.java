package com.ipinyou;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FlowSumMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    /**
     * 拿到日志中的数据,截取手机号,上行流量,下行流量,然后封装发送
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //拿一行数据
        String line = value.toString();
        //切分成各个字段
        String[] fields = StringUtils.split(line, '\t');
        String phoneNB = fields[1];
        long u_flow = Long.parseLong(fields[7]);
        long d_flow = Long.parseLong(fields[8]);
        context.write(new Text(phoneNB),new FlowBean(phoneNB,u_flow,d_flow));
    }
}
