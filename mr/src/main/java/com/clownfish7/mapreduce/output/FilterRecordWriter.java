package com.clownfish7.mapreduce.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author You
 * @create 2021-03-06 21:55
 */
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream aOut = null;
    FSDataOutputStream bOut = null;

    public FilterRecordWriter(TaskAttemptContext job) {

        // 1 获取文件系统
        FileSystem fs;

        try {
            fs = FileSystem.get(job.getConfiguration());

            // 2 创建输出文件路径
            Path aPath = new Path("D:\\you\\data\\filterOutput\\clownfish.log");
            Path bPath = new Path("D:\\you\\data\\filterOutput\\other.log");

            // 3 创建输出流
            aOut = fs.create(aPath);
            bOut = fs.create(bPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        // 判断是否包含“atguigu”输出到不同文件
        if (key.toString().contains("clownfish7")) {
            aOut.write(key.toString().getBytes());
        } else {
            bOut.write(key.toString().getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 关闭资源
        IOUtils.closeStream(aOut);
        IOUtils.closeStream(bOut);
    }
}
