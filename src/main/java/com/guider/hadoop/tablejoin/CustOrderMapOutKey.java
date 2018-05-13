package com.guider.hadoop.tablejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustOrderMapOutKey implements WritableComparable<CustOrderMapOutKey> {
    private int custId;
    private int orderId;

    public CustOrderMapOutKey() {
    }

    public CustOrderMapOutKey(int custId, int orderId) {
        this.custId = custId;
        this.orderId = orderId;
    }

    public int getCustId() {
        return custId;
    }

    public void setCustId(int custId) {
        this.custId = custId;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    @Override
    public int compareTo(CustOrderMapOutKey o) {
        int res = custId = o.getCustId();
        return res == 0 ? orderId - o.getOrderId() : res;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(custId);
        dataOutput.writeInt(orderId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        custId = dataInput.readInt();
        orderId = dataInput.readInt();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CustOrderMapOutKey) {
            CustOrderMapOutKey o = (CustOrderMapOutKey)obj;
            return custId == o.custId && orderId == o.orderId;
        } else {
            return false;
        }
    }
    @Override
    public String toString() {
        return custId + " " + orderId;
    }
}
