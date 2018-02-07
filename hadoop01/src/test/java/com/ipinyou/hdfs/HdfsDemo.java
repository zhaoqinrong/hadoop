package com.ipinyou.hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

public class HdfsDemo {
   private FileSystem fs;
    private String rootPath;
    Configuration conf;

    @Before
    public void begin() throws IOException, InterruptedException, URISyntaxException {
        //默认加载classpath下的配置文件
         conf = new Configuration();
        String s = conf.get("fs.defaultFS");
        rootPath="hdfs://node05:9000";
        fs = FileSystem.get(new URI(rootPath),conf,"root");
    }

    @After
    public void end()  {
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件夹
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        Path path = new Path(rootPath+"/hi");
        boolean exists = fs.exists(path);


        if (!exists) {
            boolean mkdirs = fs.mkdirs(path);
            System.out.println(mkdirs);
        }
        fs.delete(path,true);
        System.out.println(fs.getUri());
    }

    /**
     * 上传
     * @throws IOException
     */
    @Test
    public void upload() throws IOException {
        Path path=new Path(rootPath+"/hi/test.txt");
        FSDataOutputStream outputStream = fs.create(path);

        FileUtils.copyFile(new File("D://test.txt"),outputStream);
    }

    /**
     * 下载
     * @throws IOException
     */
    @Test
    public void download() throws IOException {
        Path path=new Path(rootPath+"/hi/text.txt");
        Path localPath=new Path("D://t.txt");
       fs.copyToLocalFile(path,localPath);

    }

    /**
     * 查看
     * @throws IOException
     */
    @Test
    public  void list() throws IOException {
        Path path = new Path(rootPath + "/hi");
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus status : fileStatuses) {
            System.out.println(status.getOwner());
            System.out.println(status.getPath());
            System.out.println(status.getLen());
            System.out.println("=====================");
        }
    }

    /**
     * 小文件合并成大文件
     * @throws Exception
     */
    @Test
    public void upload2() throws Exception {
        Path path = new Path(rootPath + "/hi/seq");
        SequenceFile.Writer writer=SequenceFile.createWriter(fs,conf,path, Text.class,Text.class);
        File file=new File("D://test");
        for(File f:file.listFiles()){
            writer.append(new Text(f.getName()),new Text(FileUtils.readFileToString(f)));
        }


    }
    /**
     * 小文件合并成大文件上传后,进行下载
     * @throws IOException
     */
    @Test
    public void download2() throws Exception {
        Path path=new Path(rootPath+"/hi/seq");
        SequenceFile.Reader reader=new SequenceFile.Reader(fs,path,conf);
        Text key=new Text();
        Text value=new Text();
        while (reader.next(key,value)){
            System.out.println(key+"==========="+value);
            System.out.println("=======================");
        }
    }

}
