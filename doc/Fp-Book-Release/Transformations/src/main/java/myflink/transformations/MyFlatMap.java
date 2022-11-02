package myflink.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class MyFlatMap {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //准备数据,类型DataStreamSource
        DataSet<String> dataSource = env.fromElements("Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams."
                );
        //转化处理数据

        final FlatMapOperator<String, String>  flatMap = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out)
                    throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        });

        flatMap.print();
    }


}


