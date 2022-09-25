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
public class AggregateonGroupedTupleDataSet {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, String, Double>> input = env.fromElements(
                new Tuple3(1, "a", 1.0),
                new Tuple3(2, "b", 2.0),
                new Tuple3(4, "b", 4.0),
                new Tuple3(3, "c", 3.0));
        DataSet<Tuple3<Integer, String, Double>> output1 = input
                .groupBy(1) //在字段2上进行分组
                .aggregate(SUM, 0).and(MIN, 2);//产生字段0的总和和字段2的最小值原始数据集

        DataSet<Tuple3<Integer, String, Double>> output2 = input
                .groupBy(1) //在字段2上进行分组
                .aggregate(SUM, 0).aggregate(MIN, 2);//在聚合上应用聚合,将在计算按字段1分组的字段0的总和后产生字段2的最小值
        output1.print();
        System.out.println("--------");

        output2.print();
    }
}
