package com.clownfish7.mapreduce.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * classname OneShareFriendsMapper
 * description TODO
 * create 2021-03-08 19:09
 *
 * @author yzy yuzhiyou999@outlook.com
 * @version 1.0
 */
public class OneShareFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = line.split(":");

        String person = fields[0];

        String[] friends = fields[1].split(",");

        for (String friend : friends) {
            context.write(new Text(friend), new Text(person));
        }

    }
}
