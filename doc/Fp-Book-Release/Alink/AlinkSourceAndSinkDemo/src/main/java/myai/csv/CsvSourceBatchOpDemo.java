package myai.csv;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.LibSvmSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.LibSvmSourceBatchOp;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class CsvSourceBatchOpDemo {

    public static void main(String[] args) throws Exception {
        //使用CsvSourceBatchOp读取数据
        String filePath = "src/main/resources/records.csv";
        CsvSourceBatchOp source = new CsvSourceBatchOp()
        //设置文件地址
                .setFilePath(filePath)
                //设置列名分别为label和review，数据类型分别为整型和字符串类型
                .setSchemaStr("label int, review string")
                //该CSV数据第一行保存的是列名，需要设置读取数据时忽略第一行
                .setIgnoreFirstLine(true);
        //输出前5条数据
        source.firstN(5).print();
        //对数据进行取样
        source.sampleWithSize(3).print();
    }

}
