package org.example.chapter_5;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: john
 * @Date: 2022-10-09-8:03
 * @Description: 流处理
 */
public class DataStreamApiProfessinal {




    /*
     * @desc: union
     * @author Administrator
     * @date 2022/10/9 8:06
     * @param ``
     * @return
     * @version 1.0.0
     */
    public static void unionDemoV1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> source1 = env.fromElements(
                new Tuple2<>("HONDA", 15),
                new Tuple2<>("CROWN", 25)
                );

        DataStreamSource<Tuple2<String, Integer>> source2 = env.fromElements(
                new Tuple2<>("BMW", 35),
                new Tuple2<>("Tesla", 40)
        );

        DataStreamSource<Tuple2<String, Integer>> source3 = env.fromElements(
                new Tuple2<>("Rolls-Royce", 300),
                new Tuple2<>("AMG", 330)
        );
        DataStream<Tuple2<String, Integer>> union = source1.union(source2, source3);
        union.print("union");
        env.execute();
    }




    /*
     * @desc: connect
     * @author Administrator
     * @date 2022/10/9 8:22
     * @param ``
     * @return
     * @version 1.0.0
     */
    public static void connectDemoV1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> source1 = env.fromElements(
                new Tuple2<>("HONDA", 15),
                new Tuple2<>("CROWN", 25)
        );

        DataStreamSource<Tuple2<String, Integer>> source2 = env.fromElements(
                new Tuple2<>("BMW", 35),
                new Tuple2<>("Tesla", 40)
        );
        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connectedStreams = source1.connect(source2);
        connectedStreams.getFirstInput().print("union1");
        connectedStreams.getSecondInput().print("union2");
        env.execute();
    }



    /*
     * @desc: reduce
     * @author Administrator
     * @date 2022/10/9 8:25
     * @param ``
     * @return
     * @version 1.0.0
     */
    public static void reduceDemoV1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = env.fromElements(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 3),
                new Tuple2<>("C", 6),
                new Tuple2<>("A", 5),
                new Tuple2<>("B", 8)
        );
        dataStreamSource
                .keyBy(tuple -> tuple.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return new Tuple2<>(stringIntegerTuple2.f0,stringIntegerTuple2.f1 + t1.f1);
                    }
                })
                .print("reduce");
        env.execute();
    }




    /*
     * @desc: split和select 算子
     * @author Administrator
     * @date 2022/10/9 8:38
     * @param ``
     * @return
     * @version 1.0.0
     */
    public static void splitDemoV1() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> input = environment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
        SplitStream<Integer> splitStream = input.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {

                List<String> output = new ArrayList<>();
                if (value % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        });
        DataStream<Integer> even = splitStream.select("even");
        DataStream<Integer> odd = splitStream.select("odd");
        even.print("even");
        odd.print("odd");
        environment.execute();
    }


    /*
     * @desc: 低阶流处理算子
     * @author Administrator
     * @date 2022/10/9 10:08
     * @param ``
     * @return
     * @version 1.0.0
     */
    public static void infoMsg(){

    }




    public static void main(String[] args) throws Exception {
//            unionDemoV1();
//            connectDemoV1();
//            reduceDemoV1();
//            splitDemoV1();

    }


}
