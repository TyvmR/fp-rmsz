package myflink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class Pojo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
         DataSet<MyCar> input  = env.fromElements(
                new MyCar("BMW", 3000),
                new MyCar("Tesla", 4000),
                 new MyCar("Tesla", 400),
                new MyCar("Rolls-Royce", 200));


        final FilterOperator<MyCar> output = input.filter(new FilterFunction<MyCar>() {
            @Override
            public boolean filter(MyCar value) throws Exception {
                return value.amount > 1000;
            }
        });

        output.print();
    }

    public static class MyCar {

        public String brand;
        public int amount;

        public MyCar() {
        }

        public MyCar(String brand, int amount) {
            this.brand = brand;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "MyCar{" +
                    "brand='" + brand + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }


}
