package org.example.fllikpartbook.chapter4_3;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * Copyright (C), 2021-2021
 * FileName: CustomAccumulator
 * Author:   Administrator
 * Date:     2021/12/10 16:36
 * Description:
 */

public class CustomAccumulator {

    public static void main(String[] args) {
         ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
         DataSource<String> stringDataSource  = executionEnvironment.fromElements("john", "ian", "rayen");
         MapOperator<String, String> result = stringDataSource.map(new RichMapFunction<String, String>() {
            IntCounter intCounter = new IntCounter();

            @Override
            public String map(String value) throws Exception {
                intCounter.add(2);
                return value;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("counter", intCounter);
            }
        });
         result.writeAsText("file.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        JobExecutionResult executeInfo = null;
        try {
            executeInfo = executionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        final int        counter     = executeInfo.getAccumulatorResult("counter");
        System.out.println(counter);
    }
}


