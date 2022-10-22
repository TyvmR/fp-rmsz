package org.example.parametes;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * @Author: john
 * @Date: 2022-10-08-16:01
 * @Description: 累加器测试
 */
public class AccumulatorDemo {


    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> input = env.fromElements("BMW", "Tesla", "Rolls-Royce");
        DataSink<Object> output = input.map(new RichMapFunction<String, Object>() {
                IntCounter intCounter = new IntCounter();
                    @Override
                    public Object map(String s) throws Exception {
                        intCounter.add(1);
                        return s;
                    }
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        getRuntimeContext().addAccumulator("myCounter",intCounter);
                    }
                }).writeAsText("c:\\file.txt", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        JobExecutionResult result = env.execute("myJob");
        int count = result.getAccumulatorResult("myCounter");
        System.out.println(count);
    }
}
