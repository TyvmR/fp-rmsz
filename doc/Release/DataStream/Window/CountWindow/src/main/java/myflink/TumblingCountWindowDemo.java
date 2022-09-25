package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class TumblingCountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Tuple2<String,Integer>> input = env.fromElements(
                Tuple2.of("S1",1),
                Tuple2.of("S1",2),
                Tuple2.of("S1",3),
                Tuple2.of("S2",4),
                Tuple2.of("S2",5),
                Tuple2.of("S2",6),
                Tuple2.of("S3",7),
                Tuple2.of("S3",8),
                Tuple2.of("S3",9)
        );

        input.keyBy(0)
                .countWindow(3)

                .sum(1)
                .print();
            env.execute();
    }

}
