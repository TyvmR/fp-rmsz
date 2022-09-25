package myflink.transformations;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MyMapPartition {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> textLines = env.fromElements("xxxx xxx","222s");
        DataSet<Long> counts = textLines.mapPartition(new PartitionCounter());
        counts.print();
    }

    public static class PartitionCounter implements MapPartitionFunction<String, Long> {

        public void mapPartition(Iterable<String> values, Collector<Long> out) {
            long c = 0;
            for (String s : values) {
                c++;
            }
            out.collect(c);
        }
    }
}
