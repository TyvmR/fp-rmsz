package myflink.transformations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyUnionDemo {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //准备数据,类型DataStreamSource
     DataStream<Tuple2<String,Integer>> source = senv.fromElements(
             new Tuple2<>("zhang",20),
             new Tuple2<>("wang",20),
             new Tuple2<>("liu",50),
             new Tuple2<>("yue",10),
             new Tuple2<>("li",10),
             new Tuple2<>("liu",5),
             new Tuple2<>("long",10));
        DataStream<Tuple2<String,Integer>> source2 = senv.fromElements(
                new Tuple2<>("z",20),
                new Tuple2<>("w",20),
                new Tuple2<>("l",50),
                new Tuple2<>("y",10),
                new Tuple2<>("l",10),
                new Tuple2<>("l",5),
                new Tuple2<>("l",10));
        source.union(source2).print("union");
        senv.execute();
    }

}
