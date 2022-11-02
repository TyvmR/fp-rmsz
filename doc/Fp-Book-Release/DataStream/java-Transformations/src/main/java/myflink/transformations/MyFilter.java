package myflink.transformations;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

public class MyFilter {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //准备数据,类型DataStreamSource
        DataSet<Tuple1<String>> dataSource = env.fromElements(Tuple1.of("利川豆豉 零食")
                , Tuple1.of("武汉豆皮 好吃")
                , Tuple1.of("北京豆汁 过瘾")
                , Tuple1.of("东坡肉 过瘾"));
        DataSet<Tuple1<String>> ds = dataSource.filter(new MyFilterFunction());
        ds.print();
    }

    public static class MyFilterFunction extends RichFilterFunction<Tuple1<String>> {

        @Override
        public boolean filter(Tuple1<String> value) throws Exception {
            return value.f0.contains("过瘾");
        }
    }
}
