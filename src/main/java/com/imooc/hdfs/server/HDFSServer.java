package com.imooc.hdfs.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * HDFS操作
 * 1. 上传文件
 * 2. 下载文件
 * 3. 删除文件
 */
public class HDFSServer {

    private final FileSystem fs;

    public HDFSServer() throws Exception {
        /* 创建一个配置对象 */
        Configuration conf = new Configuration();
        /* 指定HDFS地址 */
        conf.set("fs.defaultFS", "hdfs://192.168.249.100:9000/");
        /* 获取操作HDFS的对象  */
        this.fs = FileSystem.get(conf);
    }

    /* 上传文件 */
    public void put(String src, String dest) throws Exception {
        /* 获取本地文件的输入流 */
        FileInputStream fis = new FileInputStream(src);
        /* 获取HDFS文件系统的输出流 */
        FSDataOutputStream fos = this.fs.create(new Path(dest));
        /* 上传文件：通过工具类把输入流拷贝到输出流文件，实现本地文件上传到HDFS系统 */
        IOUtils.copyBytes(fis, fos, 1024, true);  // 1024: 缓冲区大小；true：用完之后关闭流
    }

    /* 下载文件 */
    public void get(String src, String dest) throws Exception {
        // 获取HDFS文件的输入流
        FSDataInputStream fis = this.fs.open(new Path(src));
        // 获取本地文件系统的输出流
        FileOutputStream fos = new FileOutputStream(dest);
        // 下载文件：通过工具类把输入流拷贝到输出流文件，实现HDFS系统上的文件下载到本地
        IOUtils.copyBytes(fis, fos, 1024, true);
    }

    /* 删除文件 */
    public boolean delete(String path) throws Exception {
        // 如果删除file：第二个参数将被忽略，没有意义
        // 如果删除dir：第二个参数表示是否递归删除
        return this.fs.delete(new Path(path), true);
    }
}
