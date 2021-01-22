package com.clownfish7.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;

/**
 * classname HdfsClient
 * description TODO
 * create 2021-01-18 11:33
 *
 * @author yzy yuzhiyou999@outlook.com
 * @version 1.0
 */
public class HdfsClient {

    @Test
    public void testMkdirs() throws IOException, InterruptedException {
        // 1. 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
//        configuration.set("fs.defaultFS", "hdfs://192.168.0.24:9001");
//        FileSystem fs = FileSystem.get(configuration);
//        JVM -DHADOOP_USER_NAME=root
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.0.24:9001"), configuration, "root");

        // 2. 创建目录
        fs.mkdirs(new Path("/20210118/clownfish7"));

        // 3. 关闭资源
        fs.close();
    }
}
