package com.guider.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/*mapreducer的方式实现二次索引*/

/**
 * 创建测试数据
 * bin/hbase shells
 * create "mr_hbase_source" , "f"
 * create "mr_hbase_result" , "f"
 * put "mr_hbase_source" , "zhao" , "f:age" , "zhao25"
 * put "mr_hbase_source" , "qian" , "f:age" , "qian25"
 * put "mr_hbase_source" , "sun" , "f:age" , "sun25"
 * put "mr_hbase_source" , "li" , "f:age" , "li25"
 *
 * //运行jar包
 * yarn jar /root/lib/index.jar
 */
public class  IndexBuilder {
    public static class IndexBuilderMapper extends TableMapper<Text,Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //获取列簇或者列信息
            Text k = new Text(Bytes.toString(key.get()));
            Text v = new Text(Bytes.toString(value.getValue(getBytes("f"),getBytes("age"))));
            context.write(k,v);
        }
    }
    public static class IndexBuilderReducer extends TableReducer<Text,Text,ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text value = null;
            for(Text text:values){
                value = text;
                //新表的构建
                //rowkey -> zhao , value -> zhao25 =>
                //rowkey -> zhao|szhao25 , value -> zhao
                Put put = new Put(getBytes(key.toString() + "|" + value.toString() ));
                put.add(getBytes("f"),getBytes("age"),getBytes(value.toString()));
                context.write(null,put);
            }
        }
    }

    public static byte[] getBytes(String value) {
        return Bytes.toBytes(value);
    }
    public static void main(String[] args) throws Exception{
        //配置
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance();
        job.setJobName("hbase_mapreduce_for_index");
        job.setJarByClass(IndexBuilder.class);

        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);
        //使用工具类初始化mapper与reducer
        TableMapReduceUtil.initTableMapperJob("mr_hbase_source",scan,IndexBuilderMapper.class,Text.class,Text.class,job);
        TableMapReduceUtil.initTableReducerJob("mr_hbase_result",IndexBuilderReducer.class,job);


        boolean b = job.waitForCompletion(true);
    }
}
