package org.example.chapter_7;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: john
 * @Date: 2022-10-11-9:27
 * @Description:
 */
public class TimeWindowsV1Demo {

    public static void main(String[] args) throws Exception {
        countWindow();

    }



    public static void countWindow() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> input = environment.fromElements(
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
        input
                .keyBy(0)
                .countWindow(3)
                .sum(1)
                .print();
        environment.execute();
    }


}
