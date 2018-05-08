package com.guider.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://bigguider21.com:50070";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        //列出hdfs上
        FileStatus[] statuses = fs.listStatus(new Path("/"));
        for (FileStatus status : statuses) {
            System.out.println(status);
        }

    }
}
