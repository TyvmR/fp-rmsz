package org.example.fllikpartbook.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: john
 * @Date: 2022-09-25-22:47
 * @Description:
 */
public class BatchDemoV1 {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> stringDataSource = executionEnvironment.fromElements("Flink batch demo", "batch demo", "demo");
        DataSet<Tuple2<String, Integer>> sum =
                stringDataSource.flatMap(new LineSpliter()).groupBy(0).sum(1);
        sum.print();
    }


    static class LineSpliter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] s1 = s.split(" ");
            for (String s2 : s1) {
                collector.collect(new Tuple2<>(s2,1));
            }
        }
    }
}
