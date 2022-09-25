package myflink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class ReduceFunctionDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Long>> input = sEnv.fromElements(
                new Tuple2("BMW",2L),
                new Tuple2("BMW",2L),
                new Tuple2("Tesla",3L),
                new Tuple2("Tesla",4L)
        );

        DataStream<Tuple2<String, Long>> output= input
                .keyBy(0)
                 .countWindow(2)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    });
        output.print();
        sEnv.execute();

    }
}
