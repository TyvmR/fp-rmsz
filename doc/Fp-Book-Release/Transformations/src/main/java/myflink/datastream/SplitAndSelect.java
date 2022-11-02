package myflink.datastream;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class SplitAndSelect {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> input = sEnv.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        sEnv.setParallelism(1);
        SplitStream<Integer> split = input.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> output = new ArrayList<String>();
                if (value % 2 == 0) {
                    //返回偶数
                    output.add("even");
                }
                else {
                    //返回奇数
                    output.add("odd");
                }
                return output;
            }
        });
        DataStream<Integer> even = split.select("even");
        DataStream<Integer> odd = split.select("odd");
        DataStream<Integer> all = split.select("even","odd");
        even.print("even流");
        odd.print("odd流");
        sEnv.execute();

    }
}
