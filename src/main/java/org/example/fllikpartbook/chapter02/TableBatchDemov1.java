package org.example.fllikpartbook.chapter02;

import static org.apache.flink.table.api.Expressions.$;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

/**
 * @Author: john
 * @Date: 2022-09-26-13:44
 * @Description:
 */
public class TableBatchDemov1 {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tenv = BatchTableEnvironment.create(env);
        DataSet<MyOrder> input = env.fromElements(
                new MyOrder(1L,"BMW", 1),
                new MyOrder(2L,"Tesla", 8),
                new MyOrder(2L,"Tesla", 8),
                new MyOrder(3L,"Rolls-Royce", 20));
        Table table = tenv.fromDataSet(input);
        Table amount = table.where($("amount").isGreaterOrEqual(8));
        DataSet<MyOrder> myOrderDataSet = tenv.toDataSet(amount, MyOrder.class);
        myOrderDataSet.print();
    }
}
