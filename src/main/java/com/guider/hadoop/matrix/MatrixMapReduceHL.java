package com.guider.hadoop.matrix;

import com.guider.hadoop.utils.HDFSUtils;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MatrixMapReduceHL extends Configured implements Tool {
    public static class MatrixHLMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String name = "";
        private int rowNum = 4; //A的行数
        private int colNum = 2; //B的列数
        private int rowIndexA = 1; //A目前的行数
        private int rowIndexB = 1; //B目前的行数

        //目的，将我们的矩阵按照设计要求切分好
        //根据B矩阵来确定A矩阵应该切分数据
        //添加一个区分，第一的矩阵的value里加一个标志位a，第二个用b
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            name = ((FileSplit) context.getInputSplit()).getPath().getName();
            System.out.println(name);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] ss = value.toString().split(",");
            if (name.contains("matrixa.txt")) {
                for (int i = 1; i <= colNum; i++) {
                    Text k = new Text(ss[0] + "," + i);
                    Text aValue = new Text("a," + ss[1] + "," + ss[2]);
                    context.write(k, aValue);
                }
            } else if (name.contains("matrixb.txt")) {
                for (int i = 1; i <= rowNum; i++) {
                    Text k = new Text(i + "," + ss[1]);
                    Text aValue = new Text("b," + ss[0] + "," + ss[2]);
                    context.write(k, aValue);
                }
            }
        }
    }

    public static class MatrixHLReduce extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, String> mapA = new HashMap<>();
            Map<String, String> mapB = new HashMap<>();
            for (Text value : values) {
                String[] val = value.toString().split(",");
                if (val[0].equals("a")) {
                    mapA.put(val[1], val[2]);
                } else if (val[0].equals("b")) {
                    mapB.put(val[1], val[2]);
                }
            }
            int result = 0;
            for (String tempKey : mapA.keySet()) {
                int a = Integer.parseInt(mapA.get(tempKey));
                int b = Integer.parseInt(mapB.get(tempKey));
                result = result + a * b;
            }
            context.write(key, new IntWritable(result));
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
        job.setJarByClass(MatrixMapReduceHL.class);
        //设置input与output
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);

        //job.addCacheFile(URI.create(CUSTOMER_LIST));
        //设置map与
        //需要设置的内容类 + 输出key与value
        job.setMapperClass(MatrixHLMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //shuffle优化
//        job.setPartitionerClass(cls);
//        job.setSortComparatorClass(cls);
//        job.setCombinerClass(cls);
//        job.setGroupingComparatorClass(cls);

        //设置reduce
        job.setReducerClass(MatrixHLReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //将job交给Yarn
        boolean issucess = job.waitForCompletion(true);
        return issucess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //参数
        args = new String[]{
                "hdfs://bigguider22.com:8020/user/root/matrix_hl/input",
                "hdfs://bigguider22.com:8020/user/root/matrix_hl/output"
        };
        //跑我们的任务
        handleOutputFile(args[1]);
        int status = ToolRunner.run(conf, new MatrixMapReduceHL(), args);
        System.exit(status);
    }

    public static void handleOutputFile(String outputPath) throws IOException {
        outputPath = outputPath.replace("hdfs://bigguider22.com:8020", "");
        HDFSUtils.rmdir(outputPath);
    }
}
