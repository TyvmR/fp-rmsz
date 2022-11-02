package myai;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.recommendation.AlsPredictBatchOp;
import com.alibaba.alink.operator.batch.recommendation.AlsTopKPredictBatchOp;
import com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.MySqlSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 * 使用交替最小二乘法（ALS）来实现协同过滤式的推荐系统
 */
public class ALSDemo {

    public static void main(String[] args) throws Exception {

        String url = "src/main/resources/ratings.csv";

        String schema = "user_id bigint, product_id bigint, rating double, timestamp string";
        //使用CsvSourceBatchOp读取数据
        BatchOperator data = new CsvSourceBatchOp()
                //设置文件地址
                .setFilePath(url)
                // 设置列名
                .setSchemaStr(schema);
        //拆分数据集
        SplitBatchOp splitter = new SplitBatchOp().setFraction(0.8);
        splitter.linkFrom(data);

        BatchOperator trainData = splitter;
        BatchOperator testData = splitter.getSideOutput(0);

        AlsTrainBatchOp als = new AlsTrainBatchOp()
                .setUserCol("user_id").setItemCol("product_id").setRateCol("rating")
                .setNumIter(10).setRank(10).setLambda(0.1);

        BatchOperator model = als.linkFrom(trainData);


        //根据用户推荐
        AlsTopKPredictBatchOp topKpredictor = new AlsTopKPredictBatchOp()
                .setUserCol("user_id").setPredictionCol("recommend").setTopK(3);

        BatchOperator topKpredictorResult = topKpredictor
                .linkFrom(model, testData.select("user_id").distinct().firstN(5));
        //输出根据用户推荐信息
        topKpredictorResult.print();
        //可以输出到MySQL,以便用于展示到Web或APP前端
 /*       MySqlSinkBatchOp mySqlSinkBatchOp = new MySqlSinkBatchOp()
                //MySQL地址
                .setIp("localhost")
                //MySQL端口
                .setPort("3306")
                //MySQL数据库
                .setDbName("flink")
                //MySQL用户名
                .setUsername("long")
                //MySQL密码
                .setPassword("long")
                //输出到MySQL的表名
                .setOutputTableName("topK_Result");
        mySqlSinkBatchOp.sinkFrom(topKpredictorResult);*/

        //预测评分
        AlsPredictBatchOp predictor = new AlsPredictBatchOp()
                .setUserCol("user_id").setItemCol("product_id").setPredictionCol("prediction_result");

        BatchOperator preditionResult = predictor.linkFrom(model, testData).select("user_id,product_id,rating, prediction_result").orderBy("user_id", 10);//.where("rating >4")
        // 输出预测评分
        preditionResult.print();
        //BatchOperator.execute();


    }
}
