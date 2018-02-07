package com.ipinyou;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable{
    private String phoneNum;
    private Long up_flow;
    private Long d_flow;
    private Long s_flow;

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public Long getUp_flow() {
        return up_flow;
    }

    public void setUp_flow(Long up_flow) {
        this.up_flow = up_flow;
    }

    public Long getD_flow() {
        return d_flow;
    }

    public void setD_flow(Long d_flow) {
        this.d_flow = d_flow;
    }

    public Long getS_flow() {
        return d_flow+up_flow;
    }

  /*  public void setS_flow(Long s_flow) {
        this.s_flow = s_flow;
    }*/

    /**
     * 将对象数据序列化到流中
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNum);
        out.writeLong(up_flow);
        out.writeLong(d_flow);
        out.writeLong(s_flow);


    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "phoneNum='" + phoneNum + '\'' +
                ", up_flow=" + up_flow +
                ", d_flow=" + d_flow +
                ", s_flow=" + s_flow +
                '}';
    }

    public FlowBean() {
    }

    public FlowBean(String phoneNum, Long up_flow, Long d_flow) {
        this.phoneNum = phoneNum;
        this.up_flow = up_flow;
        this.d_flow = d_flow;
        this.s_flow = d_flow+up_flow;
    }

    /**
     * 从数据流中反序列化出对象的数据
     * 注意:反序列化的顺序一定要和序列化时的顺序一致

     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        phoneNum = in.readUTF();
        up_flow = in.readLong();
        d_flow = in.readLong();
        s_flow = in.readLong();
    }
}
