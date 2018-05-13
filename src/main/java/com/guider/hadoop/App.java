package com.guider.hadoop;

import com.guider.hadoop.global.GlobalMapReduce;
import com.guider.hadoop.sort.SortMapReduce;
import com.guider.hadoop.utils.HDFSUtils;
import com.guider.hadoop.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public final static int TYPE_WORD_COUNT = 0;  //单词统计
    public final static int TYPE_SORT_MAP_REDUCE = 1; //排序
    public final static int TYPE_GLOBAL_MAP_REDUCE = 2; //全局排序
    public static void main(String[] args) throws Exception {
        //跑我们的任务
        int type = TYPE_GLOBAL_MAP_REDUCE;
        Tool tool = caseTool(type);
        //输入输出文件设置
        args = getParames(type);
        //输出文件处理
        handleOutputFile(args[1]);
        //任务执行
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, tool, args);
        System.exit(status);
    }

    public static String[] getParames(int type){
        String [] args =  new String[]{
                "hdfs://bigguider22.com:8020/user/root/mapreduce/input",
                "hdfs://bigguider22.com:8020/user/root/mapreduce/output"
        };
        switch (type) {
            case TYPE_WORD_COUNT:
                args = new String[]{
                        "/user/root/mapreduce/input",
                        "/user/root/mapreduce/output1"
                };
                break;
            case TYPE_SORT_MAP_REDUCE:
                args = new String[]{
                        "hdfs://bigguider22.com:8020/user/root/sort/input",
                        "hdfs://bigguider22.com:8020/user/root/sort/output"
                };
                break;
            case TYPE_GLOBAL_MAP_REDUCE:
                args = new String[]{
                        "hdfs://bigguider22.com:8020/user/root/global/input",
                        "hdfs://bigguider22.com:8020/user/root/global/output"
                };
                break;
        }
        return args;
    }
    public static Tool caseTool(int type){
        Tool tool = null;
        switch (type) {
            case TYPE_WORD_COUNT:
                tool = new WordCount();
                break;
            case TYPE_SORT_MAP_REDUCE:
                tool = new SortMapReduce();
                break;
            case TYPE_GLOBAL_MAP_REDUCE:
                tool = new GlobalMapReduce();
                break;
            default:
                tool = new WordCount();

        }
        return tool;
    }
    public static void handleOutputFile(String outputPath) throws IOException {
        outputPath = outputPath.replace("hdfs://bigguider22.com:8020","");
        HDFSUtils.rmdir(outputPath);
    }
}
