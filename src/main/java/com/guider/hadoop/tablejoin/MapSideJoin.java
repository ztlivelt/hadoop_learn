package com.guider.hadoop.tablejoin;

import com.guider.hadoop.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
//1.构建一个新的bean
//2.在map执行之前，将文件放进内存
//3.reduce直接输出结果
public class MapSideJoin extends Configured implements Tool {
    private static final String CUSTOMER_LIST = "hdfs://bigguider22.com:8020/user/root/sidejoin_map/input_user/user.txt";

    public static class MsjMapper extends Mapper<LongWritable, Text, CustOrderMapOutKey, Text>{
        private final Map<Integer,UserBean> userBeanMap = new HashMap<>();
        // map之前的准备工作，用来提高效率
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 小表读取，并传输
            FileSystem fs = FileSystem.get(URI.create(CUSTOMER_LIST),context.getConfiguration());
            FSDataInputStream fdis = fs.open(new Path(CUSTOMER_LIST));
            // 进行一下数据验证
            BufferedReader br = new BufferedReader(new InputStreamReader(fdis));
            String line = null;
            String[] splits = null;
            while ((line = br.readLine()) != null){
                splits = line.split(" ");
                // 进行数据是否完整的验证，亦即是说，splits是否为4个
                UserBean userBean= new UserBean();
                userBean.setUserId(Integer.valueOf(splits[0]));
                userBean.setUserName(splits[1]);
                userBean.setAddress(splits[2]);
                userBean.setPhoneNum(splits[3]);
                userBeanMap.put(userBean.getUserId(),userBean);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split(" ");
            // 构建一下我们的数据
            int custID = Integer.parseInt(splits[1]);// 通过客户编号获取信息
            UserBean userBean = userBeanMap.get(custID);
            if (userBean == null){
                return;
            }
            StringBuffer sb = new StringBuffer();
            sb.append(splits[2]).append(" ").append(userBean.toString());
            Text outputValue = new Text(sb.toString());
            CustOrderMapOutKey outputKey = new CustOrderMapOutKey(custID,Integer.parseInt(splits[2]));
            context.write(outputKey,outputValue);
        }
    }
    public static class MsjReduce extends Reducer<CustOrderMapOutKey, Text, CustOrderMapOutKey, Text>{
        @Override
        protected void reduce(CustOrderMapOutKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //原样输出
            for(Text value :values) {
                context.write(key, value);
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
        job.setJarByClass(MapSideJoin.class);
        //设置input与output
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);

//        job.addCacheFile(URI.create(CUSTOMER_LIST));
        //设置map与
        //需要设置的内容类 + 输出key与value
        job.setMapperClass(MsjMapper.class);
        job.setMapOutputKeyClass(CustOrderMapOutKey.class);
        job.setMapOutputValueClass(Text.class);

        //shuffle优化
//        job.setPartitionerClass(cls);
//        job.setSortComparatorClass(cls);
//        job.setCombinerClass(cls);
//        job.setGroupingComparatorClass(cls);

        //设置reduce
        job.setReducerClass(MsjReduce.class);
        job.setOutputKeyClass(CustOrderMapOutKey.class);
        job.setOutputValueClass(Text.class);

        //将job交给Yarn
        boolean issucess = job.waitForCompletion(true);
        return issucess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //参数
        args = new String[] {
                "hdfs://bigguider22.com:8020/user/root/sidejoin_map/input",
                "hdfs://bigguider22.com:8020/user/root/sidejoin_map/output"
        };
        //跑我们的任务
        handleOutputFile(args[1]);
        int status = ToolRunner.run(conf,new MapSideJoin(),args);
        System.exit(status);
    }
    public static void handleOutputFile(String outputPath) throws IOException {
        outputPath = outputPath.replace("hdfs://bigguider22.com:8020","");
        HDFSUtils.rmdir(outputPath);
    }
}
