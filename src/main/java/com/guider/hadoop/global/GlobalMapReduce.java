package com.guider.hadoop.global;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 自动生成数据脚本
 * createData.sh
 * #!/bin/sh
 * for i in {1..100};do
 * echo $RANDOM
 * done;
 */
public class GlobalMapReduce extends Configured implements Tool {
    public static class GlobalMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            IntWritable outputKeyandvalue = new IntWritable(Integer.valueOf(value.toString()));
            context.write(outputKeyandvalue, outputKeyandvalue);
        }
    }

    public static class GlobalReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(value, NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //获取我们的配置
        Configuration conf = this.getConf();
        if (conf == null) {
            conf = new Configuration();
        }
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());

        //设置input与output
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);

        //设置Map
        job.setMapperClass(GlobalMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle优化
        //自定义的目的，维持以key分区的方案不变
        job.setPartitionerClass(GlobalPartitioner.class);
        //自定义的目的，维持以key分组的方案不变
//        job.setGroupingComparatorClass(NewGroupComparator.class);


        //设置reduce
        job.setReducerClass(GlobalReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(3); //设置reduce的数量，1个肯定全局排序

        //将job交给Yarn
        boolean issucess = job.waitForCompletion(true);
        return issucess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        //参数
        args = new String[]{
                "hdfs://bigguider22.com:8020/user/root/global/input",
                "hdfs://bigguider22.com:8020/user/root/global/output2"
        };
        //跑我们的任务
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new GlobalMapReduce(), args);
        System.exit(status);
    }
}
