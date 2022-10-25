package com.imooc.hdfs;

import com.imooc.hdfs.server.HDFSServer;

public class HDFSTest {

    public static void main(String[] args) throws Exception {

        HDFSServer hdfs = new HDFSServer();
        hdfs.put("./bd_hadoop_resources/files/user.txt", "/user.txt");
        hdfs.get("/README.txt", "./bd_hadoop_resources/files/README.txt");
        System.out.println(hdfs.delete("/LICENSE.txt"));
    }
}
