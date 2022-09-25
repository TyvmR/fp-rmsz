package myflink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class SlidingCountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("S1", 1),
                Tuple2.of("S1", 2),
                Tuple2.of("S1", 3),
                Tuple2.of("S2", 4),
                Tuple2.of("S2", 5),
                Tuple2.of("S2", 6),
                Tuple2.of("S3", 7),
                Tuple2.of("S3", 8),
                Tuple2.of("S3", 9)
        );


        input.keyBy(0)
                .countWindow(3, 1).sum(1)//滚动窗口，每1次事件计算最近3次总和
                .print();  //滚动窗口，每1次事件计算最近10次总和

        env.execute();
    }
    }
