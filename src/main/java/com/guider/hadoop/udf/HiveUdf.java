package com.guider.hadoop.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

public class HiveUdf extends UDF {
    public String evaluate(IntWritable result){
        String tmpresult = "";
        if (result != null){
            if (result.get() >= 0 && result.get() < 80){
                tmpresult="不及格";
            } else if (result.get() < 90){
                tmpresult = "良";
            } else {
                tmpresult = "优秀";
            }
        }else {
            tmpresult = "成绩为空，请核实";
        }

        return tmpresult;

    }
}
