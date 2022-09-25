package myai.text;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.sink.TextSinkBatchOp;
import com.alibaba.alink.operator.batch.source.TextSourceBatchOp;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class TextSourceBatchOpDemo {

    public static void main(String[] args) throws Exception {
        //数据源路径
        String inPutPath = "src/main/resources/records.txt";
        //输出数据路径
        String outPutPath = "src/main/resources/outPutRecords.txt";
        //使用TextSourceBatchOp读取数据
        TextSourceBatchOp source = new TextSourceBatchOp()
                .setFilePath(inPutPath)  // 设置文件地址
                .setTextCol("text");     // 设置文本列名称,默认为text
        //输出前5条数据
        source.firstN(3).print();
        //拆分文本数据
        SplitBatchOp splitter = new SplitBatchOp().setFraction(0.5);
        splitter.linkFrom(source);
        //打印数据
        splitter.print();
        //打印旁路数据
        splitter.getSideOutput(0).print();
        //输出数据到文件
        splitter.link(new TextSinkBatchOp().
                setFilePath(outPutPath)
                .setOverwriteSink(true)//保存操作执行时，如果目标文件已经存在，是否进行覆盖(true/false)
        );

        BatchOperator.execute();
    }

}
