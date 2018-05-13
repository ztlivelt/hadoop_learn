package com.guider.hadoop.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSUtils {
    //private static String HDFS_URL = "hdfs://bigguider22.com:8020";
    private static String HDFS_URL = "";

    public static FileSystem getFileSystem() {
        //读取配置文件
        Configuration conf = new Configuration();
        //文件系统
        FileSystem fs = null;
        String hdfsUrl = HDFS_URL;
        if (StringUtils.isBlank(hdfsUrl)) {
            try {
                //返回默认文件系统，如果在hadoop集群下运行，使用此方法可以直接获取默认文件系统
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            URI uri = null;
            try {
                //返回指定的文件系统，如果在本地测试，需要此种方法获取文件系统；
                uri = new URI(hdfsUrl.trim());
                fs = FileSystem.get(uri, conf);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return fs;
    }

    /**
     * 创建文件目录
     *
     * @param path
     */
    public static void mkdirs(String path) {
        try {
            FileSystem fs = getFileSystem();
            System.out.println("FilePath : " + path);
            //创建目录
            fs.mkdirs(new Path(path));
            //释放资源
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断目录是否存在
     *
     * @param filePath 目录路径
     * @param isCreate 不存在是否创建
     * @return
     */
    public static boolean existDir(String filePath, boolean isCreate) {
        boolean flag = false;
        if (StringUtils.isEmpty(filePath)) {
            return flag;
        }
        try {
            Path path = new Path(filePath);
            FileSystem fs = getFileSystem();
            if (isCreate) {
                if (!fs.exists(path)) {
                    fs.mkdirs(path);
                }
            }
            if (fs.isDirectory(path)) {
                flag = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * 从本地上传文件到HDFS
     *
     * @param srcFile
     * @param destPath
     * @throws Exception
     */
    public static void copyFileToHdfs(String srcFile, String destPath) throws Exception {
//        FileInputStream fis = new FileInputStream(new File(srcFile));
//        FileSystem fs = FileSystem.get(URI.create(HDFS_URL + destPath), config);
//        FileSystem fs = getFileSystem();
//        OutputStream os = fs.create(new Path(destPath));
//        IOUtils.copyBytes(fis, os, 4096, true);
//        System.out.println("copy 完成 ......");
        FileSystem fs = getFileSystem();
        fs.copyFromLocalFile(new Path(srcFile),new Path(destPath));
        fs.close();
    }

    /**
     * 5、从HDFS下载文件到本地
     *
     * @param srcFile
     * @param destPath
     * @throws Exception
     */
    public static void getFile(String srcFile, String destPath) throws Exception {
        //HDFS文件地址
        //构建filesystem
//        FileSystem fs = FileSystem.get(URI.create(file), config);
//        FileSystem fs = getFileSystem();
//        //读取文件
//        InputStream is = fs.open(new Path(srcFile));
//        IOUtils.copyBytes(is, new FileOutputStream(new File(destPath)), 2048, true);
//        System.out.println("下载完成......");
//        fs.close();

        FileSystem fs = getFileSystem();
        fs.copyToLocalFile(new Path(srcFile),new Path(destPath));
        fs.close();
    }

    /**
     * 删除文件或者文件目录
     *
     * @param path
     */
    public static void rmdir(String path) throws IOException {
        //返回FileSystem对象
        FileSystem fs = getFileSystem();
        //删除文件或者文件目录 delete(Path f)此方法已经弃用
        boolean is_delete = fs.deleteOnExit(new Path(path));
        fs.close();

    }

    /**
     * 7、读取文件的内容
     *
     * @param filePath
     * @throws IOException
     */
    public static void readFile(String filePath) throws IOException {
        FileSystem fs = getFileSystem();
        //读取文件
        InputStream is = fs.open(new Path(filePath));
        //读取文件
        IOUtils.copyBytes(is, System.out, 2048, false); //复制到标准输出流
        fs.close();
    }

    /**
     * 主方法测试
     */
    public static void main(String[] args) throws Exception {         //连接fs
        HDFSUtils.rmdir("/user/root/global/output");
        HDFSUtils.rmdir("/user/root/global/output1");
        HDFSUtils.rmdir("/user/root/global/output2");
        HDFSUtils.rmdir("/user/root/mapreduce/output");
        HDFSUtils.rmdir("/user/root/mapreduce/output1");
        HDFSUtils.rmdir("/user/root/mapreduce/output2");
        HDFSUtils.rmdir("/user/root/mapreduce/output3");
        HDFSUtils.rmdir("/user/root/mapreduce/output4");
        HDFSUtils.rmdir("/user/root/sort/output");
        HDFSUtils.rmdir("/user/root/sort/output1");
        HDFSUtils.rmdir("/user/root/sort/output2");
        HDFSUtils.rmdir("/user/root/sort/output3");
        HDFSUtils.rmdir("/user/root/sort/output4");
        System.exit(1);
//        FileSystem fs = getFileSystem();
//        System.out.println(fs.getUsed());
//        fs.copyToLocalFile(new Path("/user/root/mapreduce/output"),
//                new Path("E:\\code\\java_work\\hadoop\\hdfs"));
//        fs.close();
        //创建路径
//        mkdir("/dit2");
        //验证是否存在
//        System.out.println(existDir("/dit2",false));
        //上传文件到HDFS
//        copyFileToHdfs("G:\\testFile\\HDFSTest.txt","/dit/HDFSTest.txt");
        //下载文件到本地
//        getFile("/dit/HDFSTest.txt","G:\\HDFSTest.txt");
        // getFile(HDFSFile,localFile);
        //删除文件
//        rmdir("/dit2");
        //读取文件
//        readFile("/user/root/mapreduce/output/part-r-00000");
    }
}
