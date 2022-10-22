package org.example.chapter4_4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;

public class TuplesDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple1<String>> tuple1DataSource = executionEnvironment.fromElements(
                Tuple1.of("BMW"),
                Tuple1.of("Tesla"),
                Tuple1.of("Rolls-Royce")
        );
        tuple1DataSource.map(new MyMapFunction()).print();
    }

    public static class MyMapFunction implements MapFunction<Tuple1<String>,String>{

        @Override
        public String map(Tuple1<String> stringTuple1) throws Exception {
            return "i love " + stringTuple1.f0;
        }
    }
}
