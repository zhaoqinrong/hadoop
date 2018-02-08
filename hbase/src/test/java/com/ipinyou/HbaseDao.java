
package com.ipinyou;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HbaseDao {
    Configuration conf = null;

    @Before
    public void before() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node05,node06,node07");
    }

    /**
     * 创建表
     *
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        TableName name = TableName.valueOf("test");
        HTableDescriptor desc = new HTableDescriptor(name);
        //列族信息
        HColumnDescriptor coldesc1 = new HColumnDescriptor("base_info");
        HColumnDescriptor coldesc2 = new HColumnDescriptor("extra_info");

        desc.addFamily(coldesc1);
        desc.addFamily(coldesc2);

        boolean test = admin.tableExists("test");

        if (test) {
            return;
        }
        admin.createTable(desc);
    }

    /**
     * 删除表
     *
     * @throws Exception
     */
    @Test
    public void dropTest() throws Exception {

        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable("test");
        admin.deleteTable(Bytes.toBytes("test"));
        admin.close();
    }

    /**
     * 增加数据
     *
     * @throws Exception
     */
    @Test
    public void insertTest() throws Exception {

        HTable table = new HTable(conf, "nvshen");
        Put name = new Put(Bytes.toBytes("rk0001"));
        name.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        Put age = new Put(Bytes.toBytes("rk0001"));
        age.add(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(18));
        List<Put> puts = new ArrayList<Put>();
        puts.add(name);
        puts.add(age);
        table.put(puts);
    }

    /**
     * 修改数据
     *
     * @throws Exception
     */
    @Test
    public void updateTest() throws Exception {

        HTable table = new HTable(conf, "nvshen");
        Put name = new Put(Bytes.toBytes("rk0001"));
        name.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
        Put age = new Put(Bytes.toBytes("rk0001"));
        age.add(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(18));
        List<Put> puts = new ArrayList<Put>();
        puts.add(name);
        puts.add(age);
        table.put(puts);
    }

    /**
     * 删除一行数据
     *
     * @throws Exception
     */
    @Test
    public void delTest() throws Exception {
        HTable table = new HTable(conf, "nvshen");
        Delete del = new Delete(Bytes.toBytes("rk0001"));
        table.delete(del);
    }

    /**
     * 查询一行数据
     *
     * @throws Exception
     */
    @Test
    public void get() throws Exception {
        HTable table = new HTable(conf, "nvshen");
        Get get = new Get(Bytes.toBytes("rk0001"));
        //查询到一行
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
//从result中取出指定的值
            byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name"));
            byte[] value1 = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
            System.out.println();
            System.out.println(new String(value));
            List<KeyValue> list = result.list();
            for (KeyValue keyValue : list) {
                byte[] family = keyValue.getFamily();
                byte[] key = keyValue.getQualifier();
                byte[] value2 = keyValue.getValue();
                System.out.println(new String(family) + "===>" + new String(key) + "=====" + new String(value2));

            }
       /* byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name"));
        System.out.println(new String(value));*/


        }

    }

    /**
     * 遍历整个表
     * @throws Exception
     */
    @Test
    public void scan() throws Exception{
        HTable table = new HTable(conf, "nvshen");
        Scan scan=new Scan();

        table.getScanner(scan);
    }
}
