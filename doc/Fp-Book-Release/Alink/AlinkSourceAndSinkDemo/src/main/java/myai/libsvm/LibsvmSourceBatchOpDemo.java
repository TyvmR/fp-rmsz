package myai.libsvm;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.sink.LibSvmSinkBatchOp;
import com.alibaba.alink.operator.batch.sink.TextSinkBatchOp;
import com.alibaba.alink.operator.batch.source.LibSvmSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TextSourceBatchOp;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class LibsvmSourceBatchOpDemo {

    public static void main(String[] args) throws Exception {
        //数据源路径
        String inPutPath = "src/main/resources/records.libsvm";
        //输出数据路径
        String outPutPath = "src/main/resources/outPutRecords.libsvm";
        //使用LibSvmSourceBatchOp读取数据
        LibSvmSourceBatchOp source = new LibSvmSourceBatchOp()
                .setFilePath(inPutPath);  // 设置文件地址
        //输出前5条数据
        source.firstN(3).print();

        // 对原始的数据采样2条数据
        BatchOperator batchOperator = source.sampleWithSize(2);
        //打印采样数据
        batchOperator.print();

        //输出数据到文件
        batchOperator.link(new LibSvmSinkBatchOp()
                .setFilePath(outPutPath)
                .setLabelCol("label")//标签列名称
                .setVectorCol("features")//特征数据列名称
                .setOverwriteSink(true));//保存操作执行时，如果目标文件已经存在，是否进行覆盖(true/false)

        BatchOperator.execute();
    }

}
