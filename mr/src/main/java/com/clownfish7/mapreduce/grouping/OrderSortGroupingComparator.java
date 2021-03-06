package com.clownfish7.mapreduce.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * @author You
 * @create 2021-03-06 21:36
 */
public class OrderSortGroupingComparator extends WritableComparator {

    protected OrderSortGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        int result;
        if (aBean.getOrderId() > bBean.getOrderId()) {
            result = 1;
        } else if (aBean.getOrderId() < bBean.getOrderId()) {
            result = -1;
        } else {
            result = 0;
        }

        return result;
    }

}
