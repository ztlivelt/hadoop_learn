package com.guider.hadoop.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 1. 打包 Jar
 * 2. 上传 Jar
 * 3. 运行 yarn jar ***.jar  inputpuah outputpath
 * <p>
 * 二次排序发生时期
 * <key,valu> -> <key#value> ,value -> <kay,value>
 * 发生时期，shuffic
 * 优点，发生在内存性能高
 * <p>
 * 不建议使用的方法
 * 在reduce阶段进行
 */
public class SortMapReduce extends Configured implements Tool {


    public static class SortMapper extends Mapper<LongWritable, Text, PairWritable, IntWritable> {
        private PairWritable mapOutputKey = new PairWritable();
        private IntWritable mapOutputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linevalue = value.toString();
            String[] strs = linevalue.split(",");
            if (strs.length != 2) {
                return;
            }
            mapOutputKey.setKey(strs[0]);
            mapOutputKey.setValue(Integer.valueOf(strs[1]));
            mapOutputValue.set(Integer.valueOf(strs[1]));
            context.write(mapOutputKey, mapOutputValue);
        }
    }

    //2. Reduce class
    //reducer，map的输出就是reduce的输入
    public static class SortReduce extends Reducer<PairWritable, IntWritable, Text, IntWritable> {
        private Text outputKey = new Text();

        @Override
        protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                outputKey.set(key.getKey());
                context.write(outputKey, value);
            }
//           不建议使用，reuduce阶段进行排序
//            List<IntWritable> results = new ArrayList<>();
//            for (IntWritable value : values) {
//                results.add(value);
//            }
//            Collections.sort(results);
//            for (IntWritable value : results) {
//                context.write(new Text(), value);
//            }
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
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle优化
        //自定义的目的，维持以key分区的方案不变
        job.setPartitionerClass(NewPartitioner.class);
        //自定义的目的，维持以key分组的方案不变
        job.setGroupingComparatorClass(NewGroupComparator.class);

        //设置reduce
        job.setReducerClass(SortReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //将job交给Yarn
        boolean issucess = job.waitForCompletion(true);
        return issucess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        //参数
        args = new String[]{
                "hdfs://bigguider22.com:8020/user/root/sort/input",
                "hdfs://bigguider22.com:8020/user/root/sort/output4"
        };
        //跑我们的任务
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new SortMapReduce(), args);
        System.exit(status);
    }
}
