package com.guider.hadoop.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区方案
 * 默认采用哈希取余
 */
public class NewPartitioner extends Partitioner<PairWritable, IntWritable> {
    @Override
    public int getPartition(PairWritable pairWritable, IntWritable intWritable, int i) {
        //保持以Key来进行partition进行分区，而不是新的key#value
        //采用默认分区方式

        return (pairWritable.getKey().hashCode() & Integer.MAX_VALUE) & i;
    }
}
