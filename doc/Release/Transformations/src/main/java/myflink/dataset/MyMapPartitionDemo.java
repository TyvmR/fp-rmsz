package myflink.dataset;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class MyMapPartitionDemo {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> textLines = env.fromElements("BMW","Tesla","Rolls-Royce");
        DataSet<Long> counts = textLines.mapPartition(new PartitionCounter());
        counts.print();
    }

    private static class PartitionCounter  implements MapPartitionFunction<String, Long> {


        @Override
        public void mapPartition(Iterable<String> values, Collector<Long> out) throws Exception {
            long i = 0;
            for (String value : values) {
                i++;
            }
            out.collect(i);
        }
    }
}

