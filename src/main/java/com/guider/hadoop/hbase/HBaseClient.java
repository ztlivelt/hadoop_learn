package com.guider.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseClient {
    public static HTable getTable(String tname) throws Exception {
        //获取配置，使用HBaseConfiguration
        Configuration conf = HBaseConfiguration.create();
        //操作的表
        HTable table = new HTable(conf, tname);
        return table;
    }

    public static void getData(HTable table,String rowkey) throws Exception {
        //实例化一个get，指定一个rowkey
        Get get = new Get(Bytes.toBytes(rowkey));
        //get某列的值
        //get.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age"));
        //get一个列簇
        get.addFamily(Bytes.toBytes("f1"));

        //定义一个result
        Result rs = table.get(get);
        //打印数据
        for (Cell cell : rs.rawCells()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append(Bytes.toString(CellUtil.cloneFamily(cell)))
                    .append("***")
                    .append(Bytes.toString(CellUtil.cloneQualifier(cell)))
                    .append("***")
                    .append(Bytes.toString(CellUtil.cloneValue(cell)))
                    .append("***")
                    .append(cell.getTimestamp());
            System.out.println(buffer);
            System.out.println("------------------");
        }

    }

    public static void putData(HTable table,String rowkey) throws Exception {
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(getBytes("f1"), getBytes("sex"), getBytes("male"));
        table.put(put);
        getData(table,rowkey);
    }

    public static void deleteData(HTable table,String rowkey) throws Exception {
        Delete del = new Delete(getBytes(rowkey));
        del.deleteColumn(getBytes("f1"), getBytes("sex"));
        //del.deleteFamily(getBytes("f1"));
        table.delete(del);
        getData(table,rowkey);
    }

    public static void scanData(HTable table) throws Exception {
        Scan scan = new Scan();
        ResultScanner rescan = table.getScanner(scan);
        for (Result rs : rescan) {
            for (Cell cell : rs.rawCells()) {
                StringBuffer buffer = new StringBuffer();
                buffer.append(Bytes.toString(CellUtil.cloneFamily(cell)))
                        .append("***")
                        .append(Bytes.toString(CellUtil.cloneQualifier(cell)))
                        .append("***")
                        .append(Bytes.toString(CellUtil.cloneValue(cell)))
                        .append("***")
                        .append(cell.getTimestamp());
                System.out.println(buffer);
            }
            System.out.println("----------------------------");
        }
    }

    public static void rangeData(HTable table) throws Exception {
        Scan scan = new Scan();
        // conf the scan
        //scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));
        scan.setStartRow(Bytes.toBytes("2018_1002"));
        scan.setStopRow(Bytes.toBytes("2018_1003"));
        ResultScanner rsscan = table.getScanner(scan);
        for (Result rs : rsscan){
            for (Cell cell : rs.rawCells()){
                StringBuffer buffer = new StringBuffer();
                buffer.append(Bytes.toString(CellUtil.cloneFamily(cell)))
                        .append("***")
                        .append(Bytes.toString(CellUtil.cloneQualifier(cell)))
                        .append("***")
                        .append(Bytes.toString(CellUtil.cloneValue(cell)))
                        .append("***")
                        .append(cell.getTimestamp());
                System.out.println(buffer);
            }
            System.out.println("---------------------------");
        }
    }

    public static byte[] getBytes(String value) {
        return Bytes.toBytes(value);

    }

    public static void main(String[] args) throws Exception {
        HTable table = getTable("ns1:t2");
        //get数据
//        getData(table,"2018_1001");
        //添加数据
//        putData(table,"2018_1004");
//
//        //删除数据
//        deleteData(table,"2018_1004");
//
//        //scan数据
        scanData(table);
//
//        //scan范围查看
//        rangeData(table);
    }
}
