package com.clownfish7.mapreduce.friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * classname TwoShareFriendsReducer
 * description TODO
 * create 2021-03-08 19:26
 *
 * @author yzy yuzhiyou999@outlook.com
 * @version 1.0
 */
public class TwoShareFriendsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        for (Text friend : values) {
            stringBuffer.append(friend).append(" ");
        }
        context.write(new Text(key), new Text(stringBuffer.toString()));
    }
}
