package com.guider.hadoop.hbase;

import com.guider.hadoop.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

public class Hbasebulkload extends Configured implements Tool {



    /*
     * input->map->shuffle->reduce->output
     */

    //mapper,输入数据变成键值对，一行转化为一条
    public static class HbasebulkloadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        private IntWritable mapOutputKey = new IntWritable();
        private IntWritable mapOutputValue = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // line value
            String lineValue = value.toString();
            //value进行切分
            String[] valueStrSplit = lineValue.split(",");
            String hkey = valueStrSplit[0];
            String name = valueStrSplit[1];
            String sex = valueStrSplit[2];
            String age = valueStrSplit[3];
            byte[] rowKey = Bytes.toBytes(hkey);
            ImmutableBytesWritable rk = new ImmutableBytesWritable(rowKey);
            //HBASE_ROW_KEY,'basicinfo:name','basicinfo:sex','basicinfo:age'

            String family = "basicinfo";
            String column_name = "name";
            String column_sex = "sex";
            String column_age = "age";

            //传递出去
            Put hput_name = new Put(rowKey);
            hput_name.add(getBytes(family), getBytes(column_name), getBytes(name));
            context.write(rk, hput_name);

            Put hput_sex = new Put(rowKey);
            hput_sex.add(getBytes(family), getBytes(column_sex), getBytes(sex));
            context.write(rk, hput_sex);

            Put hput_age = new Put(rowKey);
            hput_age.add(getBytes(family), getBytes(column_age), getBytes(age));
            context.write(rk, hput_age);
        }
    }


    //driver:任务相关设置
    public int run(String[] args) throws Exception {
        //获取相关配置
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(Hbasebulkload.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        //设置input
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);
        //设置output
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);

        //设置map
        job.setMapperClass(HbasebulkloadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);

        HTable hTable = new HTable(conf, "stu_info");
        HFileOutputFormat2.configureIncrementalLoad(job, hTable);

        //将job提交给Yarn
        boolean isSuccess = job.waitForCompletion(true);
        if (isSuccess) {
            FsShell shell = new FsShell(conf);
            try {
                shell.run(new String[]{"-chmod", "-R", "777", args[1]});
            } catch (Exception e) {
                throw new IOException(e);
            }
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(outpath, hTable);
        }
        if (hTable != null) {
            hTable.close();
        }
        return isSuccess ? 0 : 1;
    }

    public static byte[] getBytes(String value) {
        return Bytes.toBytes(value);

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        //保存的输入输出路径
        args = new String[]{
                "hdfs://ns/user/root/hbase",
                "hdfs://ns/HBaseFile"
        };
        handleOutputFile(args[1]);
        //将任务跑起来
        //int statas = new WordCountMapReduce().run(args);
        int statas = ToolRunner.run(conf, new Hbasebulkload(), args);
        //关闭我们的job
        System.exit(statas);
    }

    public static void handleOutputFile(String outputPath) throws IOException {
        outputPath = outputPath.replace("hdfs://ns", "");
        HDFSUtils.rmdir(outputPath);
    }
}

