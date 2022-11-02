package myflink.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

public class MyFlatMap {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //准备数据,类型DataStreamSource
        DataSet<Tuple1<String>> dataSource = env.fromElements(Tuple1.of("利川豆豉 零食")
                , Tuple1.of("武汉豆皮 好吃")
                , Tuple1.of("北京豆汁 过瘾"));
        //转化处理数据
        DataSet<Tuple1<String>> ds= dataSource.flatMap(new MyFlatMapFunction());
        ds.print();



    }

    public static class MyFlatMapFunction implements FlatMapFunction <Tuple1<String>,Tuple1<String>>{

        @Override
        public void flatMap(Tuple1<String> value, Collector<Tuple1<String>> out) throws Exception {
            for (String s : value.f0.split(" ")) {
                out.collect(new Tuple1<String>(s));
            }
        }
    }
}


