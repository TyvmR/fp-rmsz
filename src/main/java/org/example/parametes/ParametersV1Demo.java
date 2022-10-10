package org.example.parametes;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * @Author: john
 * @Date: 2022-10-08-14:39
 * @Description:
 */
public class ParametersV1Demo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> integerDataSource = executionEnvironment.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        Configuration configuration = new Configuration();
        configuration.setInteger("limit",5);
        integerDataSource.filter(new RichFilterFunction<Integer>() {

            private int limit;


            @Override
            public void open(Configuration parameters) throws Exception {
                limit = parameters.getInteger("limit", 0);

            }

            @Override
            public boolean filter(Integer integer) throws Exception {

                return  integer > limit;
            }
        }).withParameters(configuration)
                .print();
    }
}
