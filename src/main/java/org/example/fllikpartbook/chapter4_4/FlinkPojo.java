package org.example.fllikpartbook.chapter4_4;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;

/**
 * Copyright (C), 2021-2021
 * FileName: FlinkPojo
 * Author:   Administrator
 * Date:     2021/12/15 21:18
 * Description:
 */

public class FlinkPojo {


    public static void main(String[] args) throws Exception {
         ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
         DataSource<MyCar> myCarDataSource = executionEnvironment.fromElements(
                new MyCar("bmw", 30000),
                new MyCar("benz", 40000),
                new MyCar("toyato", 1000)

        );
         FilterOperator<MyCar> filter = myCarDataSource.filter(new FilterFunction<MyCar>() {
            @Override
            public boolean filter(MyCar value) throws Exception {
                return value.getAmount() > 1000;
            }
        });
         filter.print();
    }



    public static class MyCar{
        private String brand;
        private int amount;

        public String getBrand() {
            return brand;
        }

        public void setBrand(String brand) {
            this.brand = brand;
        }

        public int getAmount() {
            return amount;
        }

        public void setAmount(int amount) {
            this.amount = amount;
        }

        public MyCar(String brand, int amount) {
            this.brand = brand;
            this.amount = amount;
        }

        public MyCar(){

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


