package com.guider.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.List;

/**
 * 协处理器方式
 * Coprocessor
 * 实现二级索引
 * 1. 打包jar
 * 2. hdfs中创建存放jar的文件夹（hdfs dfs mkdir -p /jar_dir）
 * 3. 将jar导入到hdfs系统中 （hdfs dfs -put /root/datas/hbase/hadoop.jar /jar_dir）
 * 4. 进入hbase环境: /root/modules/hbase-0.98.6-hadoop2/bin/hbase shell：
 * 5. 创建测试数据
 * create 'tb_coprecessor_result' , 'familyName'
 * create 'tb_coprecessor_source' , 'familyName'
 * disable 'tb_coprecessor_source'
 * alter 'tb_coprecessor_source' , METHOD => 'table_att' , 'coprocessor' => 'hdfs://ns/jar_dir/hadoop.jar|com.guider.hadoop.hbase.IndexBuilderCoprocessor|1001'
 *
 * enable 'tb_coprecessor_source'
 * put 'tb_coprecessor_source' , 'a' , 'familyName:columnName' , 'avalue'
 * put 'tb_coprecessor_source' , 'b' , 'familyName:columnName' , 'bvalue'
 * put 'tb_coprecessor_source' , 'c' , 'familyName:columnName' , 'cvalue'
 *
 * 7. 查看运行结果：
 * scan 'tb_coprecessor_source'
 * ROW  COLUMN+CELL
 *  a   column=familyName:columnName, timestamp=1531145100929, value=avalue
 *  b   column=familyName:columnName, timestamp=1531145101196, value=bvalue
 *  c   column=familyName:columnName, timestamp=1531145103218, value=cvalue
 * 3 row(s) in 0.0410 seconds
 *
 * hbase(main):011:0> scan "tb_coprecessor_result"
 * ROW       COLUMN+CELL
 *  avalue    column=familyName:columnName, timestamp=1531145100620, value=a
 *  bvalue    column=familyName:columnName, timestamp=1531145100895, value=b
 *  cvalue    column=familyName:columnName, timestamp=1531145103006, value=c
 */
public class IndexBuilderCoprocessor extends BaseRegionObserver {

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //配置
        Configuration conf = new Configuration();
        HTable table = new HTable(conf,"tb_coprecessor_result");

        //处理器
        List<Cell> keyValue = put.get("familyName".getBytes(),"columnName".getBytes());
        for (Cell temCell : keyValue){
            final byte[] value = temCell.getValue();
            Put newPut = new Put(value);
            newPut.add("familyName".getBytes(),"columnName".getBytes(),temCell.getRow());
            table.put(newPut);
        }
        table.close();
    }

}
