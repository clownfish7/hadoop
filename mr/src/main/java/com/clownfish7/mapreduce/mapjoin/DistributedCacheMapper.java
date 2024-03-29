package com.clownfish7.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @author You
 * @create 2021-03-07 19:00
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1 获取缓存的文件
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
        String line;
        while (StringUtils.isNotBlank(line = reader.readLine())) {
            // 2 切割
            String[] fields = line.split("\t");

            // 3 缓存数据到集合
            pdMap.put(fields[0], fields[1]);

        }
        // 4 关流
        reader.close();

    }

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();

        // 2 截取
        String[] fields = line.split("\t");

        // 3 获取产品id
        String pId = fields[1];

        // 4 获取商品名称
        String pdName = pdMap.get(pId);

        // 5 拼接
        k.set(line + "\t" + pdName);

        // 6 写出
        context.write(k, NullWritable.get());

    }
}
