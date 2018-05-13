package com.guider.hadoop.sort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组方式
 */
public class NewGroupComparator implements RawComparator<PairWritable> {
    /**
     *
     * @param b1 需要比较的字节数组
     * @param s1 第一个字节数组进行比较的尾部位置
     * @param l1 第一个字节比到PairWritable的前一个字节
     * @param b2
     * @param s2
     * @param l2
     * @return
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1, 0, l1 - 4, b2, 0, l2 - 4);
    }

    @Override
    public int compare(PairWritable o1, PairWritable o2) {
        return o1.getKey().compareTo(o2.getKey());
    }
}
