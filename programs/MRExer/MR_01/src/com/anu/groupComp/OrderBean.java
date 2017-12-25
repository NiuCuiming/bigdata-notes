package com.anu.groupComp;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
订单类
 */
public class OrderBean implements WritableComparable<OrderBean> {

    //定单属性
    private String orderId;
    private String produceId;
    private String tradeCount;  //成交金额

    public OrderBean() {
    }

    public OrderBean(String orderId, String produceId, String tradeCount) {
        this.orderId = orderId;
        this.produceId = produceId;
        this.tradeCount = tradeCount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProduceId() {
        return produceId;
    }

    public void setProduceId(String produceId) {
        this.produceId = produceId;
    }

    public String getTradeCount() {
        return tradeCount;
    }

    public void setTradeCount(String tradeCount) {
        this.tradeCount = tradeCount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(produceId);
        dataOutput.writeUTF(tradeCount);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        orderId	= dataInput.readUTF();
        produceId= dataInput.readUTF();
        tradeCount = dataInput.readUTF();

    }

    @Override
    public int compareTo(OrderBean o) {

        //是否商品ID相等，
        int cmp = this.produceId.compareTo(o.getProduceId());
        if(cmp == 0) {

            //再根据成交额比较
            cmp = -this.tradeCount.compareTo(o.tradeCount);
        }
        return cmp;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "orderId='" + orderId + '\'' +
                ", produceId='" + produceId + '\'' +
                ", tradeCount='" + tradeCount + '\'' +
                '}'+System.lineSeparator();
    }
}
