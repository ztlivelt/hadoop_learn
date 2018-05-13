package com.guider.hadoop.global;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区，按照数字区间分区
 */
public class GlobalPartitioner extends Partitioner<IntWritable,IntWritable> {
    @Override
    public int getPartition(IntWritable intWritable, IntWritable intWritable2, int i) {
        int code = intWritable.get();
        if (code < 10000){
            return 0;
        } else if (code < 20000){
            return 1;
        } else {
            return 2;
        }
    }
}
