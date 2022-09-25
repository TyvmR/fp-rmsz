package myflink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */

public class ForwardedFieldDemo{
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, Integer>> input = env.fromElements(
                Tuple2.of(1, 2)
        );
        input.map(new MyMap())
                .print();
    }

}
// 转发Tuple2的字段1到Tuple3的字段3
@FunctionAnnotation.ForwardedFields("f0->f2")
class MyMap implements MapFunction<Tuple2<Integer, Integer>, Tuple3<String, Integer, Integer>> {

    @Override
    public Tuple3<String, Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
        return new Tuple3<String, Integer, Integer>("foo", value.f1*8, value.f0);
    }
}