package com.clownfish7.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * classname WordcountReducer
 * description TODO
 * create 2021-01-18 14:30
 *
 * @author yzy yuzhiyou999@outlook.com
 * @version 1.0
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    int sum;
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1. 累加求和
        sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }

        // 2. 输出
        v.set(sum);
        context.write(key, v);
    }
}
