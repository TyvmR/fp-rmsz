package myai.text;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.SplitStreamOp;
import com.alibaba.alink.operator.stream.sink.TextSinkStreamOp;
import com.alibaba.alink.operator.stream.source.TextSourceStreamOp;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class TextSourceStreamOpDemo {

    public static void main(String[] args) throws Exception {
        //使用CsvSourceBatchOp读取URL数据
        String inPutPath = "src/main/resources/records.txt";
        String outPutPath = "src/main/resources/outPutRecords2.txt";
        TextSourceStreamOp source = new TextSourceStreamOp()
                //设置文件地址
                .setFilePath(inPutPath)
                .setTextCol("text");//设置文本列名称,默认为text
        //输出数据
        source.print();
        //拆分文本数据
        SplitStreamOp splitter = new SplitStreamOp().setFraction(0.5);
        splitter.linkFrom(source);
        splitter.print();
        splitter.getSideOutput(0).print();
        //输出文本
        splitter.link(new TextSinkStreamOp().setFilePath(outPutPath).setOverwriteSink(true));

        StreamOperator.execute();
    }

}
