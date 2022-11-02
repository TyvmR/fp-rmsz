package myflink.transformations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyFilterDemo {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //准备数据,类型DataStreamSource
     DataStream<Integer> source = senv.fromElements(1, 2, 3, 4, 5)
             .filter(new FilterFunction<Integer>() {
                 @Override
                 public boolean filter(Integer value) throws Exception {
                     return value<3;
                 }
             });
        source.print("Filter");
        senv.execute("Filter Demo");

    }


}
