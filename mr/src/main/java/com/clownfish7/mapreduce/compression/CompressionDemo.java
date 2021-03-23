package com.clownfish7.mapreduce.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * classname CompressionDemo
 * description TODO
 * create 2021-03-08 11:34
 *
 * @author yzy yuzhiyou999@outlook.com
 * @version 1.0
 */
public class CompressionDemo {
    public static void main(String[] args) throws Exception {

        /**
         * default  org.apache.hadoop.io.compress.DefaultCodec
         * gzip     org.apache.hadoop.io.compress.GzipCodec
         * bzip2    org.apache.hadoop.io.compress.BZip2Codec
         */
//        compress("d:/123.txt", "org.apache.hadoop.io.compress.GzipCodec");
        decompress("d:/123.txt.gz");
        decompress("d:/123.txt.deflate");
    }

    /**
     * 压缩
     */
    private static void compress(String fileName, String method) throws Exception {
        // 1 获取输入流
        FileInputStream fis = new FileInputStream(fileName);

        Class codecClass = Class.forName(method);

        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());

        // 2 获取输出流
        FileOutputStream fos = new FileOutputStream(fileName + codec.getDefaultExtension());
        CompressionOutputStream cos = codec.createOutputStream(fos);

        // 3 流的拷贝
        IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);

        // 4 关闭资源
        cos.close();
        fos.close();
        fis.close();

    }

    /**
     * 解压
     */
    private static void decompress(String fileName) throws Exception {
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = codecFactory.getCodec(new Path(fileName));
        if (null == codec) {
            System.out.println("cannot find codec for file " + fileName);
            return;
        }
        // 1 获取输入流
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(fileName));

        // 2 获取输出流
        FileOutputStream fos = new FileOutputStream(fileName + ".decoded");

        // 3 流的拷贝
        IOUtils.copyBytes(cis, fos, 1024 * 1024 * 5);

        // 4 关闭资源
        fos.close();
        cis.close();
    }
}
