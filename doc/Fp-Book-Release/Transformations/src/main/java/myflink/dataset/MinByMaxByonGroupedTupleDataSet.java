package myflink.dataset;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import static org.apache.flink.api.java.aggregation.Aggregations.MIN;
import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class MinByMaxByonGroupedTupleDataSet {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, String, Double>> input = env.fromElements(
                new Tuple3(1, "a", 1.0),
                new Tuple3(2, "b", 2.0),
                new Tuple3(4, "b", 4.0),
                new Tuple3(5, "b", 1.0),
                new Tuple3(3, "c", 3.0));
        DataSet<Tuple3<Integer, String, Double>> output1 = input
                .groupBy(1)   // 在第2个字段上进行分组
                .minBy(0, 2); // 根据第1个和第3个字段，选择最小值的元祖
        output1.print();
    }
}
