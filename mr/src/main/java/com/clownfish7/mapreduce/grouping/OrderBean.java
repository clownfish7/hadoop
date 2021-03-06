package com.clownfish7.mapreduce.grouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author You
 * @create 2021-03-06 21:31
 */
public class OrderBean implements WritableComparable<OrderBean> {

    /**
     * 订单id号
     */
    private int orderId;
    /**
     * 价格
     */
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(int orderId, double price) {
        super();
        this.orderId = orderId;
        this.price = price;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readInt();
        price = dataInput.readDouble();

    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

    // 二次排序
    @Override
    public int compareTo(OrderBean o) {

        int result;

        if (orderId > o.getOrderId()) {
            result = 1;
        } else if (orderId < o.getOrderId()) {
            result = -1;
        } else {
            // 价格倒序排序
            result = price > o.getPrice() ? -1 : 1;
        }

        return result;
    }

}
