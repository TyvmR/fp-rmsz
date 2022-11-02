package myflink;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class NonForwardedFieldDemo {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Integer, Integer, Integer>> input = env.fromElements(
                Tuple4.of(1,2,3,4));
        input.map(new MyMap()).print();
    }
    @FunctionAnnotation.ReadFields("f0; f3")
    // f0和f3由该函数读取和评估
    static   class MyMap implements MapFunction<Tuple4<Integer, Integer, Integer, Integer>,
                Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Tuple4<Integer, Integer, Integer, Integer> val) {
            if(val.f0 == 2) {
                return new Tuple2<Integer, Integer>(val.f0, val.f1);
            } else {
                return new Tuple2<Integer, Integer>(val.f3+8, val.f1+8);
            }
        }
    }
}

