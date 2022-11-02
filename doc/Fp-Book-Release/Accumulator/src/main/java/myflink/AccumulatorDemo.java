package myflink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class AccumulatorDemo {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env= ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> input = env.fromElements("BMW", "Tesla", "Rolls-Royce");
        DataSet<String> result =input.map(new RichMapFunction<String,String>() {
            @Override
            public String map(String value) throws Exception {
                //使用累加器
                //如果并行度为1，则使用普通的累加求和即可；如果设置多个并行度，则普通的累加求和结果就不准
                intCounter.add(1);
                return value;
            }

                 //创建累加器
            IntCounter intCounter = new IntCounter();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //注册累加器
                getRuntimeContext().addAccumulator("myAccumulatorName", intCounter);
            }

        });

        result.writeAsText("d:\\file.TXT", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        JobExecutionResult jobExecutionResult = env.execute("myJob");
        //获取累加器的计数结果，参数是累加器的名称，不是intCounter的名称
       int  accumulatorResult=jobExecutionResult.getAccumulatorResult("myAccumulatorName");
        System.out.println(accumulatorResult);
    }
}
