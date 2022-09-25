package myflink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class BulkIterationDemo {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //  创建初始的IterativeDataSet
        IterativeDataSet<Integer> initial = env.fromElements(0).iterate(10);
        DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer i) throws Exception {
                double x = Math.random();
                double y = Math.random();
                return i + ((x * x + y * y < 1) ? 1 : 0);
            }
        });
        // 迭代转换IterativeDataSet
        DataSet<Integer> count = initial.closeWith(iteration);
        count.map(new MapFunction<Integer, Double>() {
            @Override
            public Double map(Integer count) throws Exception {
                return count / (double) 10 * 4;
            }
        }).print();

    }
}
