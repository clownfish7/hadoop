package com.clownfish7.mapreduce.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * classname TwoShareFriendsMapper
 * description TODO
 * create 2021-03-08 19:26
 *
 * @author yzy yuzhiyou999@outlook.com
 * @version 1.0
 */
public class TwoShareFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String friend = fields[0];
        String[] persons = fields[1].split(",");
        Arrays.sort(persons);
        for (int i = 0; i < persons.length - 1; i++) {
            for (int j = i + 1; j < persons.length; j++) {
                context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
            }
        }
    }
}
