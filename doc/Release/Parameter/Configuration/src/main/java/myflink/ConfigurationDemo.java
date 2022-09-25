package myflink;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class ConfigurationDemo {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> input = env.fromElements(1,2,3,5,10,12,15,16);
        // Configuration类来存储参数
        Configuration configuration = new Configuration();
        configuration.setInteger("limit", 8);
        input.filter(new RichFilterFunction<Integer>() {
            private int limit;

            @Override
            public void open(Configuration configuration) throws Exception {
                limit = configuration.getInteger("limit", 0);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                return value > limit;
            }
        }).withParameters(configuration)
                .print();;

    }

}
