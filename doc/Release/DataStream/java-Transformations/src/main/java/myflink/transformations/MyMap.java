package myflink.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyMap {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //准备数据,类型DataStreamSource
        DataStream<Tuple1<String>> dataSource = senv.fromElements(Tuple1.of("利川豆豉")
                , Tuple1.of("武汉豆皮")
                , Tuple1.of("北京豆汁"));
        //转化处理数据
        DataStream<String> ds= dataSource.map(new MyMapFunction());
        ds.print();
        senv.execute();



    }
    public static class MyMapFunction implements MapFunction<Tuple1<String>,String> {
        @Override
        public String map(Tuple1<String> value) throws Exception {
            return "我爱"+value.f0;
        }
    }
}


