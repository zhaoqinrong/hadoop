package com.ipinyou;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class HdfsDemo {
    private Configuration conf;
    private FileSystem fileSystem;
    private String rootPath;

    @Before
    public void before() throws IOException, URISyntaxException, InterruptedException {
        conf = new Configuration();
        rootPath = "hdfs://node05:9000/";
        fileSystem = FileSystem.get(new URI("hdfs://node05:9000/a.txt"), conf, "root");
    }

    /**
     * 上传文件,比较底层
     */
    @Test
    public void upload() throws Exception {
        Path path = new Path(rootPath + "a.txt");
        FSDataOutputStream out = fileSystem.create(path);
        FileInputStream in = new FileInputStream("E:\\input.txt");
        IOUtils.copy(in, out);

    }

    /**
     * 上传文件,封装好的写法
     */
    @Test
    public void upload2() throws IOException {
        fileSystem.copyFromLocalFile(new Path("E:\\java电子书\\Netty In Action中文版.pdf"),new Path(rootPath+"netty.pdf"));
    }

    /**
     * 下载文件
     */
 @Test
    public void download() throws IOException {
    fileSystem.copyToLocalFile(false,new Path(rootPath+"netty.pdf"),new Path("E:\\Netty In Action中文版.pdf"),true);
    }

    /**
     * 查看文件信息
     */
@Test
    public void list() throws IOException {
    RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(rootPath),true);
    while (files.hasNext()){
        System.out.println(files.next().getPath().getName());
    }

}

    /**
     * 创建目录
     */
    @Test
    public void mkdir() throws IOException {
    fileSystem.mkdirs(new Path(rootPath+"/bbbb/ccccc"));
    }

    /**
     * 删除文件
     */
    @Test
    public void rm() throws IOException {
fileSystem.delete(new Path(rootPath+"/bbbb/ccccc"),true);
    }

}
