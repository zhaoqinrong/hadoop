package com.ipinyou;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.awt.event.FocusEvent;
import java.io.IOException;

/**
 *4个泛型前两个是指定mapper输入数据的类型,一个key 一个value,后两个是指定reduce输出的类型
 * map 和 reduce的数据输入输出都是以key和value的形式封装
 * 默认情况下,框架传给我们的mapper 的输入数据中,key是文本中一行的起始偏移量,这行的内容为value
 */
public class WCMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
    /**
     * mapreduce框架每读一行数据就调用一次该方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //具体业务逻辑就写在这个方法体重,而且我们业务要处理的数据,已经被框架传递过来,在方法的参数中
        //key 是这一行数据的起始偏移量 value 是这一行的文本内容

        //这一行的内容转换成String
        String line = value.toString();
        //对这一行的文本按特定分隔符切分
        String[] words = StringUtils.split(line, ' ');

        //遍历这个单词组,按照输出格式KV形式,key单词,value 1
        for (String word:words){
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
