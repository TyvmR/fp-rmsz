package myflink.datastream;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment();
        sEnv.setParallelism(1);

        DataStream<Tuple2<String, Integer>> input = sEnv.fromElements(
                new Tuple2<>("Honda", 15),
                new Tuple2<>("Honda", 10),
                new Tuple2<>("CROWN", 25),
                new Tuple2<>("BMW", 35));

        KeyedStream<Tuple2<String, Integer>, Tuple> output = input.keyBy(0);

        output.print();
        sEnv.execute();
    }
}