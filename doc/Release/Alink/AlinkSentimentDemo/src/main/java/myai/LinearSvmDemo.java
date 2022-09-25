package myai;

import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.classification.LinearSvm;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.dataproc.Imputer;
import com.alibaba.alink.pipeline.nlp.DocCountVectorizer;
import com.alibaba.alink.pipeline.nlp.Segment;
import com.alibaba.alink.pipeline.nlp.StopWordsRemover;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class LinearSvmDemo {

    public static void main(String[] args) throws Exception {
        //使用CsvSourceBatchOp读取URL数据
        String trainData = "src/main/resources/TrainData.csv";
        //根据各列的定义，组装schemaStr
        String schemaStr = "label int, review string";
        CsvSourceBatchOp source = new CsvSourceBatchOp()
                //设置文件地址
                .setFilePath(trainData)
                //设置列名分别为label和review，数据类型分别为整型和字符串类型
                .setSchemaStr(schemaStr)
                //该CSV数据第一行保存的是列名，需要设置读取数据时忽略第一行
                .setIgnoreFirstLine(true);
        //输出前5条数据
        source.firstN(5).print();

        //设置管道，将整个处理和模型过程封装在管道里
        Pipeline pipeline = new Pipeline(
                //对“review”列进行缺失值填充:填充字符串值“null”，结果写到“reviewOutput“列
                new Imputer()
                        .setSelectedCols("review")
                        .setOutputCols("reviewOutput")
                        .setStrategy("value")
                        .setFillValue("null"),
                //进行分词操作，将原句子分解为单词，之间用空格分隔。分词结果会直接替换调输入列的值
                new Segment()
                        .setSelectedCol("reviewOutput"),
                //将分词结果中的停用词去掉
                new StopWordsRemover()
                        .setSelectedCol("reviewOutput"),
                //对“reviewOutput“列出现的单词进行统计，并根据计算出的TF值，将句子映射为向量，向量长度为单词个数，并保存在"featureVector"列
                new DocCountVectorizer()
                        .setFeatureType("TF")//支持IDF/WORD_COUNT/TF_IDF/Binary/TF
                        .setSelectedCol("reviewOutput")
                        .setOutputCol("featureVector"),
                //使用LogisticRegression分类模型。分类预测放在“pre”列
                new LinearSvm()
                        .setVectorCol("featureVector")
                        .setLabelCol("label")
                        .setPredictionCol("pre")
        );

        String testData = "src/main/resources/TestData.csv";
        CsvSourceBatchOp testSource = new CsvSourceBatchOp()
                //设置文件地址
                .setFilePath(testData)
                //设置列名分别为label和review，数据类型分别为整型和字符串类型
                .setSchemaStr(schemaStr)
                //该CSV数据第一行保存的是列名，需要设置读取数据时忽略第一行
                .setIgnoreFirstLine(true);

        //通过Pipeline的fit()方法，可以得到整个流程的模型（PipelineModel）
        PipelineModel pipelineModel = pipeline.fit(source);
        //调用transform()方法进行预测（推理）
        pipelineModel.save("src/main/resources/LinearSvmSentimentModel.csv");
        pipelineModel.transform(testSource)
                //输出数据
                .select(new String[]{"pre", "label", "review"})
                .firstN(10)
                .print();
    }

}
