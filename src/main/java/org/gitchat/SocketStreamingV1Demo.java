package org.gitchat;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: john
 * @Date: 2022-10-27-23:35
 * @Description:
 */
public class SocketStreamingV1Demo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum =
                dataStreamSource.flatMap(new LineSpilter()).keyBy(0).sum(1);
        sum.print();
        env.execute();
    }


    public static final class LineSpilter implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if(token.length() > 0){
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
