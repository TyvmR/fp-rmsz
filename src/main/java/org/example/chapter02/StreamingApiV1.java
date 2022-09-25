package org.example.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.expressions.Rand;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Author: john
 * @Date: 2022-09-25-23:02
 * @Description:
 */
public class StreamingApiV1 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> dataStream = executionEnvironment
                .addSource(new MySourceFunction())
                .flatMap(new LineSpliter())
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .sum(1);
        dataStream.print();
        executionEnvironment.execute();
    }


    static class MySourceFunction implements SourceFunction<String> {

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (true){
                List<String> stringLists = new ArrayList<>();
                stringLists.add("world");
                stringLists.add("flink");
                stringLists.add("Stream");
                stringLists.add("Batch");
                stringLists.add("Table");
                stringLists.add("Sql");
                stringLists.add("hello");
                int size = stringLists.size();
                int i = new Random().nextInt(size);
                String s = stringLists.get(i);
                sourceContext.collect(s);
                Thread.sleep(1000L);

            }
        }

        @Override
        public void cancel() {

        }
    }

    static class LineSpliter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] s1 = s.split(" ");
            for (String s2 : s1) {
                collector.collect(new Tuple2<>(s2,1));
            }
        }
    }
}
