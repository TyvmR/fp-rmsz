package org.example.parametes;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * @Author: john
 * @Date: 2022-10-08-16:48
 * @Description:
 */
public class TupleV1Demo {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple1<String>> dataset = env.fromElements(
                Tuple1.of("BMW"),
                Tuple1.of("BENZ"));
                dataset.map(new MyMapFunction()).print();

    }


    public static class MyMapFunction implements MapFunction<Tuple1<String>,String> {

        @Override
        public String map(Tuple1<String> value) throws Exception {
            return  "I love " + value.f0;
        }
    }
}
