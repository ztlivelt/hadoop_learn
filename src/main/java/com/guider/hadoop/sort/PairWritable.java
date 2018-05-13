package com.guider.hadoop.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements WritableComparable<PairWritable> {
    private String key;
    private int value;

    public PairWritable() {
    }

    public PairWritable(String key, int value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(PairWritable o) {
        int compa = this.key.compareTo(o.getKey());
        if (compa == 0){
            return this.value - o.getValue();
        }
        return compa;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(key);
        dataOutput.writeInt(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        key = dataInput.readUTF();
        value = dataInput.readInt();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
