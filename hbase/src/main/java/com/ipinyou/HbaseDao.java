package com.ipinyou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;

import java.io.IOException;

public class HbaseDao {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin =new HBaseAdmin(conf);


        TableName name = TableName.valueOf("nvshen1");
        HTableDescriptor desc = new HTableDescriptor(name);
        HColumnDescriptor base_info = new HColumnDescriptor("base_info");
        HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");
        base_info.setMaxVersions(5);
        desc.addFamily(base_info);
        desc.addFamily(extra_info);

        admin.createTable(desc);

    }
}
