package myflink.dataset;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class FirstN {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, Integer>> in = env.fromElements(
                Tuple2.of("BMW", 30),
                Tuple2.of("Tesla", 35),
                Tuple2.of("Tesla", 55),
                Tuple2.of("Tesla", 80),
                Tuple2.of("Rolls-Royce", 300),
                Tuple2.of("BMW", 40),
                Tuple2.of("BMW", 45),
                Tuple2.of("BMW", 80)
        );
        //返回前2个元素
        DataSet<Tuple2<String, Integer>> out1 = in.first(2);
        //返回分组中的前2个元素
        DataSet<Tuple2<String, Integer>> out2 = in.groupBy(0)
                .first(2);

        DataSet<Tuple2<String, Integer>> out3 = in.groupBy(0)//根据字段1分组
                .sortGroup(1, Order.ASCENDING)//根据字段2排序
                .first(2);//返回每个分组的前2个元素，并且按照升序排列
        out1.print();
        System.out.println("-------------");
        out2.print();
        System.out.println("-------------");
        out3.print();

    }
}
