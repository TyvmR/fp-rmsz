package myflink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class Tuples {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //准备数据
        DataSet<Tuple1<String>> dataSource = env.fromElements(Tuple1.of("BMW")
                , Tuple1.of("Tesla")
                , Tuple1.of("Rolls-Royce"));
        //转化处理数据
        DataSet<String> ds= dataSource.map(new MyMapFunction());
        ds.print();

    }
    public static class MyMapFunction implements MapFunction<Tuple1<String>,String> {
        @Override
        public String map(Tuple1<String> value) throws Exception {
            return "I love "+value.f0;
        }
    }
}
