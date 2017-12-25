package com.anu.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
用来描述订单的Bean
 */
public class OrderBean implements Writable {

    private String id;
    private String date;
    private String amount;
    private String pid;
    private String name;
    private String category_id;
    private String price;

    public OrderBean(String id, String date, String amount, String pid, String name, String category_id, String price) {
        this.id = id;
        this.date = date;
        this.amount = amount;
        this.pid = pid;
        this.name = name;
        this.category_id = category_id;
        this.price = price;
    }

    public OrderBean(OrderBean orderBean) {

        this.id = orderBean.getId();
        this.date = orderBean.getDate();
        this.amount = orderBean.getAmount();
        this.pid = orderBean.getPid();
        this.name = orderBean.getName();
        this.category_id = orderBean.getCategory_id();
        this.price = orderBean.getPrice();

    }

    public OrderBean() { }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCategory_id() {
        return category_id;
    }

    public void setCategory_id(String category_id) {
        this.category_id = category_id;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "id='" + id + '\'' +
                ", date='" + date + '\'' +
                ", amount='" + amount + '\'' +
                ", pid='" + pid + '\'' +
                ", name='" + name + '\'' +
                ", category_id='" + category_id + '\'' +
                ", price='" + price + '\'' +
                '}'+System.lineSeparator();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(id);
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(amount);
        dataOutput.writeUTF(pid);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(category_id);
        dataOutput.writeUTF(price);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        id = dataInput.readUTF();
        date = dataInput.readUTF();
        amount = dataInput.readUTF();
        pid = dataInput.readUTF();
        name = dataInput.readUTF();
        category_id	 = dataInput.readUTF();
        price = dataInput.readUTF();


    }
}
