package com.clownfish7.mapreduce.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author You
 * @create 2021-02-12 17:47
 */
public class KVTextMapper extends Mapper<Text, Text, Text, IntWritable> {

    // 1 设置value
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // 2 写出
        context.write(key, v);
    }

}
