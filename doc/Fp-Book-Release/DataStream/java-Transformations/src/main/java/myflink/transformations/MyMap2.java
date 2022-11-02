package myflink.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyMap2 {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, Integer>> inPut = env.fromElements(Tuple2.of(2017,4),
                Tuple2.of(1990,10));
        DataSet<Integer> intSums = inPut.map(new MyMapFunction());
        intSums.print();
    }
    public static class MyMapFunction implements MapFunction<Tuple2<Integer, Integer>, Integer> {
        @Override
        public Integer map(Tuple2<Integer, Integer> in) {
            return in.f0 + in.f1;
        }
    }
}
