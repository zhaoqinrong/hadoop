package com.ipinyou;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.Iterator;

public class FlowSumReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    /**
     * 1.mapper传递过来的数据为<phonenum,1,2,1,1,1,1,1,>,reducer要做的就是对这些统计求和
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> it = values.iterator();
        long up_flow_counter=0;
        long d_flow_counter=0;
        while (it.hasNext()){
            FlowBean bean = it.next();
          up_flow_counter+= bean.getUp_flow();
          d_flow_counter+=bean.getD_flow();
        }
        context.write(key,new FlowBean(key.toString(), up_flow_counter,d_flow_counter));
    }
}