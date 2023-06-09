package myflink.transformations;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class MyConnectDemo {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //准备数据
        DataStream<Tuple1<String>> source1 = senv.fromElements(
                new Tuple1<>("Honda"),
                new Tuple1<>("CROWN"));
        DataStream<Tuple2<String, Integer>> source2 = senv.fromElements(
                new Tuple2<>("BMW", 35),
                new Tuple2<>("Tesla", 40));


        ConnectedStreams<Tuple1<String>, Tuple2<String, Integer>> connectedStreams = source1.connect(source2);
        connectedStreams.getFirstInput().print("union");
        connectedStreams.getSecondInput().print("union");
        senv.execute();
    }

}
