package com.guider.hadoop;

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

public class WordCount extends Configured implements Tool {
    // input -> map -> shuffle -> output
    // mapper，输入数据变成键值对，一行转化为一条
    //1. Map claaa
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text mapOutputKey = new Text();
        private IntWritable mapOutputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1.将读取的文件变成，偏移量+内容
            String linevalue = value.toString();
            System.out.println("linevalue----" + linevalue);
            //2.根据“ ”某种规则划分我们的单词，并处理
            String[] strs = linevalue.split(" ");
            for (String str : strs) {
                //key:单词， value:1
                mapOutputKey.set(str);
                mapOutputValue.set(1);
                context.write(mapOutputKey, mapOutputValue);
                System.out.println("str----" + str);
            }
            //3.将结果传递出处
        }
    }

    //2. Reduce class
    //reducer，map的输出就是reduce的输入
    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable outputValue = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //汇总
            int sum = 0;
            for (IntWritable value : values){
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key,outputValue);
        }
    }

    //3. job class
    public int run(String[] args) throws Exception {
        //获取我们的配置
        Configuration conf = new Configuration();
        //Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        //设置input与output
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);

        //设置map与
        //需要设置的内容类 + 输出key与value
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle优化
//        job.setPartitionerClass(cls);
//        job.setSortComparatorClass(cls);
//        job.setCombinerClass(cls);
//        job.setGroupingComparatorClass(cls);

        //设置reduce
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //将job交给Yarn
        boolean issucess = job.waitForCompletion(true);
        return issucess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //参数
        args = new String[] {
                "hdfs://bigguider22.com:8020/user/root/mapreduce/input",
                "hdfs://bigguider22.com:8020/user/root/mapreduce/output1"
        };
        //跑我们的任务
        int status = new WordCount().run(args);
        //int status = ToolRunner.run(conf,new WordCount(),args);
        System.exit(status);
    }
}
