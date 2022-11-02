package myai;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.MySqlSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.operator.stream.dataproc.SplitStreamOp;
import com.alibaba.alink.operator.stream.evaluation.EvalBinaryClassStreamOp;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlPredictStreamOp;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlTrainStreamOp;
import com.alibaba.alink.operator.stream.sink.Kafka011SinkStreamOp;
import com.alibaba.alink.operator.stream.sink.MySqlSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.operator.stream.source.MySqlSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.feature.FeatureHasher;
import com.alibaba.alink.pipeline.feature.OneHotEncoder;
import org.apache.commons.math3.analysis.function.Sigmoid;


/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */

public class FTRLDemo {

    public static void main(String[] args) throws Exception {


        //根据各列的定义，组装schemaStr  ,site_category string,
        String schemaStr
                = "click string, dt string,banner_pos string, "
                + "app_id string, app_domain string, app_category string, device_id string, "
                + "device_ip string, device_model string, C10 int, C11 int, C12 int, C13 int, "
                + "C14 int, C15 int";
        //批式处理原始训练数据
        String batchData = "src/main/resources/batchData.csv";
        //通过CsvSourceBatchOp读取显示数据
        CsvSourceBatchOp trainBatchData = new CsvSourceBatchOp()
                .setFilePath(batchData)
                .setSchemaStr(schemaStr);
        //输出数据
        trainBatchData.firstN(5).print();
        //定义标签列名，FTRL训练模型必填参数
        String labelColName = "click";
        //定义向量列名，即特征工程的结果列名称，Alink的FTRL算法默认设置的特征向量维度是30000。算法第一步是切分高维度向量，以便分布式计算
        String vecColName = "vecColName";
        System.out.println(trainBatchData.count());
        //特征的数量，是输出向量的长度
        int numHashFeatures = 30000;
        String[] selectedColNames = new String[]{
                "banner_pos",  "app_domain",
                "app_category", "C10", "C11", "C12", "C13", "C14", "C15",
                "device_id", "device_model"};
        String[] categoryColNames = new String[]{
                "banner_pos", "app_domain",
                "app_category", "device_id", "device_model"};
        String[] numericalColNames = new String[]{
                "C10", "C11", "C12", "C13", "C14", "C15"};
        // 步骤一：特征工程
        // 设置特征工程管道(工作流)
        Pipeline pipeline = new Pipeline()
                .add(
                        // 标准缩放,计算训练集的平均值和标准差，以便测试数据集使用相同的变换
                        new StandardScaler()
                                // 具有参数的类的接口，该参数指定多个表的列名称
                                .setSelectedCols(numericalColNames)
                )
                .add(
                        //特征哈希，将一些分类或数字特征投影到指定维的特征向量中
                        new FeatureHasher()
                                // 用于处理的列的名称
                                .setSelectedCols(selectedColNames)
                                // 输入表中用于训练的分类列的名称
                                .setCategoricalCols(categoryColNames)
                                // 输出列名称
                                .setOutputCol(vecColName)
                                // 特征数量。这将是输出向量的长度
                                .setNumFeatures(numHashFeatures)


                );
        // 拟合特征管道模型
        PipelineModel pipelineModel = pipeline.fit(trainBatchData);

        // 准备流训练数据
        String streamData = "src/main/resources/streamData.csv";
        CsvSourceStreamOp data = new CsvSourceStreamOp()
                .setFilePath(streamData)
                //格式化schemaStr
                .setSchemaStr(schemaStr);
        //是否忽略csv文件的第一行
        // .setIgnoreFirstLine(true);
 /*       MySqlSourceStreamOp dataMySQL = new MySqlSourceStreamOp()
                .setDbName("flink")
                .setUsername("long")
                .setPassword("long")
                .setIp("localhost")
                .setPort("3306")
                .setInputTableName("test")
                .setSchemaStr(schemaStr);*/

        // 使用拆分算子SplitStreamOp分割流为训练和评估数据
        SplitStreamOp splitter = new SplitStreamOp()
                //设置拆分比例
                .setFraction(0.9)
                .linkFrom(data);
        // 步骤二：批式模型训练
        // 训练出一个逻辑回归模型作为FTRL算法的初始模型，这是为了系统冷启动的需要
        LogisticRegressionTrainBatchOp lr = new LogisticRegressionTrainBatchOp()
                //参数vectorColName的特性
                .setVectorCol(vecColName)
                //输入表中标签列名称
                .setLabelCol(labelColName)
                // 是否具有拦截，默认为true
                .setWithIntercept(true)
                // 最大迭代次数
                .setMaxIter(10)
                /*
                线性训练的参数，优化类型
                * LBFGS,LBFGS method,大规模优化算法，默认选项
                *GD,梯度下降法(Gradient Descent method)
                *Newton,与牛顿法(Newton method)
                *SGD, 随机梯度下降法(Stochastic Gradient Descent method)
                *OWLQN，OWLQN method,该算法是单象限的L-BFGS算法，每次迭代都不会超出当前象限
                */
                .setOptimMethod("LBFGS");

        // 批式向量训练数据可以通过transform()方法得到，initModel是训练好的模型
        BatchOperator<?> initModel = pipelineModel.transform(trainBatchData).link(lr);

        // 步骤三：在线模型训练（FTRL）

        // 在初始模型基础上进行FTRL在线训练， 加载模型，FtrlTrainStreamOp将initModel作为初始化参数
        FtrlTrainStreamOp model = new FtrlTrainStreamOp(initModel)
                // 向量列的名称
                .setVectorCol(vecColName)
                // 输入表中标签列的名称
                .setLabelCol(labelColName)
                //是否具有拦截，默认为true
                .setWithIntercept(true)
                .setAlpha(0.1)
                .setBeta(0.1)
                //L1正则化参数
                .setL1(0.01)
                //L2正则化参数
                .setL2(0.01)
                //时间间隔
                .setTimeInterval(10)
                //嵌入的向量大小
                .setVectorSize(numHashFeatures)
                //获取流式向量训练数据
                .linkFrom(pipelineModel.transform(splitter));
        // 步骤四：在线预测

        // 在FTRL在线模型的基础上，连接预测数据进行预测，需要“连接”FTRL在线模型训练输出的模型流，和流式向量预测数据
        FtrlPredictStreamOp predictResult = new FtrlPredictStreamOp(initModel)
                //向量列的名称
                .setVectorCol(vecColName)
                //预测的列名
                .setPredictionCol("pred")
                //要保留在输出表中的列的名称
                .setReservedCols(new String[]{labelColName, "click"})
                //预测结果的列名称。其中将包含详细信息（预测结果的信息，例如分类器中每个标签的概率)
                .setPredictionDetailCol("details")
                // .select(new String[]{"pre", "label", "review"})
                .linkFrom(model, pipelineModel.transform(splitter.getSideOutput(0)));
        predictResult.print();


  /*      MySqlSinkStreamOp mySqlSinkStreamOp = new MySqlSinkStreamOp()
                .setDbName("flink")
                .setUsername("long")
                .setPassword("long")
                .setIp("localhost")
                .setPort("3306")
                .setOutputTableName("test_out");

        mySqlSinkStreamOp.linkFrom(predictResult);*/

      Kafka011SinkStreamOp sink = new Kafka011SinkStreamOp()
                .setBootstrapServers("localhost:9092").setDataFormat("JSON")//支持JSON和CSV
                .setTopic("mytopic");
        sink.linkFrom(predictResult);

        // 步骤五：在线评估

        // 对预测结果流进行评估，FTRL的eval将预测结果流predResult接入流式二分类评估组件EvalBinaryClassStreamOp，并设置相应的参数，由于每次评估结果给出的是Json格式，为了便于显示，还可以在后面上Json内容提取组件JsonValueStreamOp
        predictResult
                .link(
                        /**
                         * 二分类评估是对二分类算法的预测结果进行效果评估。
                         * 支持Roc曲线，LiftChart曲线，Recall-Precision曲线绘制。
                         * 流式的实验支持累计统计和窗口统计。
                         * 给出整体的评估指标包括：AUC、K-S、PRC, 不同阈值下的Precision、Recall、F-Measure、Sensitivity、Accuracy、Specificity和Kappa
                         */
                        new EvalBinaryClassStreamOp()
                                //输入表中标签列的名称
                                .setLabelCol(labelColName)
                                //预测的列名
                                .setPredictionCol("pred")
                                //预测结果的列名称，其中将包含详细信息。
                                .setPredictionDetailCol("details")
                                //流窗口的时间间隔，单位为s
                                .setTimeInterval(10)
                )
                .link(
                        /**
                         * 组件JsonValueStreamOp完成json字符串中的信息抽取，按照用户给定的Path抓取出相应的信息。该组件支持多Path抽取
                         */
                        new JsonValueStreamOp()
                                //用于处理的所选列的名称
                                .setSelectedCol("Data")
                                //要保留在输出表中的列的名称
                                .setReservedCols(new String[]{"Statistics"})
                                /**
                                 * 输出列的名称
                                 * ACCURACY:准确性;
                                 * AUC：Roc曲线下面的面积;
                                 * ConfusionMatrix：用于分类评估的混淆矩阵。横轴是预测结果值，纵轴是标签值。[TP FP] [FN TN]。根据混淆矩阵计算其他指标。
                                 */
                                .setOutputCols(new String[]{"Accuracy", "AUC", "ConfusionMatrix"})
                                //json值的参数
                                .setJsonPath(new String[]{"$.Accuracy", "$.AUC", "$.ConfusionMatrix"})
                )
                //输出评估结果：Statistics列有两个值all和window，all表示从开始运行到现在的所有预测数据的评估结果；window表示时间窗口（当前设置为10秒）的所有预测数据的评估结果。
                .print();
        // 对于流式的任务，print()方法不能触发流式任务的执行，必须调用StreamOperator.execute()方法，才能开始执行
        StreamOperator.execute();
    }
}