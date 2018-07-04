package com.guider.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HBaseMapReduce extends Configured implements Tool {
    /*
    * hbase -> hbase , 提取name这一列
    */
    public static class HBaseMapper extends TableMapper<ImmutableBytesWritable,Put>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //数据的筛选，通过操作我们封装的put来进行
            Put put = new Put(key.get());
            for (Cell cell : value.rawCells()){
                //在这里筛选出basicinfo：name这一列
                if ("basicinfo".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                    if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        put.add(cell);
                    }
                }
            }
            context.write(key,put);
        }
    }

    //driver:任务相关设置
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf,"hbase-mapreduce");
        job.setJarByClass(HBaseMapReduce.class);     // class that contains mapper and reducer

        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(
           "ns1:stu_info",   //输入的表
                scan,               // Scan instance to control CF and attribute selection
                HBaseMapper.class,  // mapper class
                ImmutableBytesWritable.class,  // mapper output key
                Put.class,  // mapper output value
                job
        );
        TableMapReduceUtil.initTableReducerJob(
                "ns1:new_stu_info",
                null,
                job
        );
        job.setNumReduceTasks(1);   // at least one, adjust as required
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess?0:1;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = HBaseConfiguration.create();
        //将任务跑起来
        //int statas = new WordCountMapReduce().run(args);
        int statas = ToolRunner.run(conf, new HBaseMapReduce(), args);
        //关闭我们的job
        System.exit(statas);
    }
}
