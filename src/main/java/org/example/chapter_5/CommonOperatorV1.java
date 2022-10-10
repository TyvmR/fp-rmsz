package org.example.chapter_5;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: john
 * @Date: 2022-10-08-19:06
 * @Description: 通用算子
 */
public class CommonOperatorV1 {

    public static void main(String[] args) throws Exception {
//        mapDemo();
//        flatMapDemo();
//        filterDemo();
            projectDemo();
    }

    //map
    public static void mapDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(4, 1, 7);
        SingleOutputStreamOperator<Integer> map = integerDataStreamSource.map(s -> s + 8);
        map.print("Map");
        env.execute();
    }


    //flatmap
    public static void flatMapDemo() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.fromElements("" +
                "hello flink demo is " +
                "you are a big foolish" +
                "you are a pig" +
                "you seem like pig" +
                "");
        FlatMapOperator<String, String> flmtOpr = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                    for (String v : s.split(" ")) {
                        collector.collect(v);
                    }
            }
        });

        flmtOpr.print();
    }

    //filter
    public static void filterDemo() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Integer> dataSource = env.fromElements(1, 2, 3, 4, 5, 6);
        FilterOperator<Integer> filter = dataSource.filter(s -> s > 5);
        filter.print();
    }

    //project
    public static void projectDemo() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple3<String, Integer, Integer>> dataSource = env.fromElements(
                new Tuple3<String, Integer, Integer>("john", 29, 1000),
                new Tuple3<String, Integer, Integer>("ian", 30, 5000),
                new Tuple3<String, Integer, Integer>("rayen", 25, 500)

        );
        ProjectOperator<?, Tuple> project = dataSource.project(2, 0);
        project.print();
    }
}
