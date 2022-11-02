package myflink.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyMapDemo {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //准备数据
        DataStream<Integer> dataSource = senv.fromElements(1, 2, 3)
                .map(x -> x + 2);
        //输出
        dataSource.print("Map");
        senv.execute("Map Demo");
    }

}


