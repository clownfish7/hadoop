package com.clownfish7.mapreduce.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author You
 * @create 2021-03-06 17:58
 */
public class CustomPartition extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String key = text.toString();
        if (key.startsWith("136")) {
            return 0;
        }
        if (key.startsWith("137")) {
            return 1;
        }
        if (key.startsWith("138")) {
            return 2;
        }
        if (key.startsWith("139")) {
            return 3;
        }
        return 4;
    }
}
