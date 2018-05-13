package com.guider.hadoop.tablejoin;

import com.guider.hadoop.utils.HDFSUtils;
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
import java.util.ArrayList;
import java.util.List;

public class ReduceSideJoin extends Configured implements Tool {
    public static class RsjMapper extends Mapper<LongWritable, Text, IntWritable, UserAndCityBean> {
        private IntWritable mapOutputKey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linevalue = value.toString();
            String[] strs = linevalue.split(" ");
            UserAndCityBean mapOutputValue = new UserAndCityBean();
            if (strs.length == 2) {
                mapOutputKey.set(Integer.valueOf(strs[0]));
                mapOutputValue.setCityId(strs[0]);
                mapOutputValue.setCityName(strs[1]);
                mapOutputValue.setFlag(0);
                System.out.println(mapOutputValue.toString());
            } else if (strs.length == 3) {
                mapOutputKey.set(Integer.valueOf(strs[2]));
                mapOutputValue.setUserId(strs[0]);
                mapOutputValue.setUserName(strs[1]);
                mapOutputValue.setCityId(strs[2]);
                mapOutputValue.setFlag(1);
            }
            context.write(mapOutputKey, mapOutputValue);
        }
    }

    public static class RsjReduce extends Reducer<IntWritable, UserAndCityBean, Text, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<UserAndCityBean> values, Context context) throws IOException, InterruptedException {
            UserAndCityBean cityBean = null;
            List<UserAndCityBean> userAndCityBeans = new ArrayList<>();
            for (UserAndCityBean value : values) {
                if (value.getFlag() == 0) {
                    System.out.println(value);
                    cityBean = new UserAndCityBean(value);
                } else {
                    userAndCityBeans.add(new UserAndCityBean(value));
                }
            }
            System.out.println(cityBean.toString());
            for (UserAndCityBean userBean : userAndCityBeans) {
                userBean.setCityName(cityBean.getCityName());
                context.write(new Text(userBean.toString()),NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //获取我们的配置
        Configuration conf = this.getConf();
        if (conf == null){
            conf = new Configuration();
        }

        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        //设置input与output
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);

        //设置map与
        //需要设置的内容类 + 输出key与value
        job.setMapperClass(RsjMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(UserAndCityBean.class);

        //shuffle优化
//        job.setPartitionerClass(cls);
//        job.setSortComparatorClass(cls);
//        job.setCombinerClass(cls);
//        job.setGroupingComparatorClass(cls);

        //设置reduce
        job.setReducerClass(RsjReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //将job交给Yarn
        boolean issucess = job.waitForCompletion(true);
        return issucess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //参数
        args = new String[] {
                "hdfs://bigguider22.com:8020/user/root/sidejoin/input",
                "hdfs://bigguider22.com:8020/user/root/sidejoin/output"
        };
        //跑我们的任务
        handleOutputFile(args[1]);
        int status = ToolRunner.run(conf,new ReduceSideJoin(),args);
        System.exit(status);
    }
    public static void handleOutputFile(String outputPath) throws IOException {
        outputPath = outputPath.replace("hdfs://bigguider22.com:8020","");
        HDFSUtils.rmdir(outputPath);
    }
}
