package myflink.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyReduceDemo {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //准备数据,类型DataStreamSource
     DataStream<Tuple2<String,Integer>> source = senv.fromElements(new Tuple2<>("long",20),
             new Tuple2<>("zhang",20),
             new Tuple2<>("wang",20),
             new Tuple2<>("yue",10),
             new Tuple2<>("li",10),
             new Tuple2<>("liu",5),
             new Tuple2<>("long",10));
        KeyedStream<Tuple2<String,Integer>,Tuple> keyed=source.keyBy(0);
        DataStream <Tuple2<String, Integer>> reduce = keyed.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);

            }
        });

        reduce.print("reduce");
        senv.execute("Reduce Demo");

    }


}
