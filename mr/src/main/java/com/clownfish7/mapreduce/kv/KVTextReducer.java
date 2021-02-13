package com.clownfish7.mapreduce.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author You
 * @create 2021-02-12 17:49
 */
public class KVTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // 1 汇总统计
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);

        // 2 输出
        context.write(key, v);
    }
}
