package com.guider.hadoop.log;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import java.net.URI;

public class LogHandlerMapReducer extends Configured implements Tool {

    public static class LogHandlerMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
        private IntWritable mapOutputKey = new IntWritable();
        private IntWritable mapOutputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineValue = value.toString();
            String[] values = lineValue.split("\t");
            String urlValue = values[1];
            if (StringUtils.isBlank(urlValue)) {
                // conuter
                context.getCounter("WEBPVMAPPER_CUUNTERS", "URL_BLANK")
                        .increment(1L);
                return;
            }
            if (36 > values.length) {
                // conuter
                context.getCounter("WEBPVMAPPER_CUUNTERS", "LENGTH_LT_30")
                        .increment(1L);

                return;
            }
            // province id
            String provinceIdValue = values[23];
            if (StringUtils.isBlank(provinceIdValue)) {
                // conuter
                context.getCounter("WEBPVMAPPER_CUUNTERS", "PROVINCEID_BLANK")
                        .increment(1L);
                return;
            }
            Integer provinceId = Integer.MAX_VALUE;
            try {
                provinceId = Integer.valueOf(provinceIdValue);
            } catch (Exception e) {
                // conuter
                context.getCounter("WEBPVMAPPER_CUUNTERS",
                        "PROVINCEID_NOT_NUMBER").increment(1L);
                return;
            }
            // map outpu key
            mapOutputKey.set(provinceId);

            context.write(mapOutputKey, mapOutputValue);

        }
    }
    public static class LogHandlerReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // temp sum
            int sum = 0;
            // iterator
            for (IntWritable value : values) {
                sum += value.get();
            }
            // set output
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        //获取相关配置
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(LogHandlerMapReducer.class);

        //设置input
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);

        //设置output
        Path outpath = new Path(args[1]);
        outpath.getFileSystem(conf).delete(outpath, true);
        FileSystem fs = FileSystem.get(new URI(outpath.toString()), conf);
        if (fs.exists(new Path(outpath.toString()))) {
            fs.delete(new Path(outpath.toString()), true);
        }
        FileOutputFormat.setOutputPath(job, outpath);

        //设置map
        job.setMapperClass(LogHandlerMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle优化
        //job.setPartitionerClass(cls);
        //job.setSortComparatorClass(cls);
        //job.setCombinerClass(cls);
        //job.setGroupingComparatorClass(cls);

        //设置reduce
        job.setReducerClass(LogHandlerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //将job提交给Yarn
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //保存的输入输出路径
        args = new String[]{
                "hdfs://lee01.cniao5.com:8020/user/root/mapreduce/input",
                "hdfs://lee01.cniao5.com:8020/user/root/mapreduce/output"
        };
        //将任务跑起来
        //int statas = new WordCountMapReduce().run(args);
        int statas = ToolRunner.run(conf, new LogHandlerMapReducer(), args);
        //关闭我们的job
        System.exit(statas);
    }
}
