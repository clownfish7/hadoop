package com.clownfish7.mapreduce.grouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author You
 * @create 2021-03-06 21:40
 */
public class OrderSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"D:\\you\\data\\orderInput", "D:\\you\\data\\orderOutput"};

        // 1. 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2. 设置jar加载路径
        job.setJarByClass(OrderSortDriver.class);

        // 3. 设置map和reduce类
        job.setMapperClass(OrderSortMapper.class);
        job.setReducerClass(OrderSortReducer.class);

        // 4. 设置map输出
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5. 设置最终输出
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8 设置reduce端的分组
        job.setGroupingComparatorClass(OrderSortGroupingComparator.class);

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
