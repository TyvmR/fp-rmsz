
package myflink.table;

import myflink.pojo.MyOrder;
import myflink.source.MySource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class TableBatchDemo {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		DataSet<MyOrder> input = env.fromElements(
				new MyOrder(1L,"BMW", 1),
				new MyOrder(2L,"Tesla", 8),
				new MyOrder(2L,"Tesla", 8),
				new MyOrder(3L,"Rolls-Royce", 20));
		//将数据集转换为表
		Table table = tEnv.fromDataSet(input);
		//过滤操作
		Table filtered = table.where($("amount").isGreaterOrEqual(8));
		DataSet<MyOrder> result = tEnv.toDataSet(filtered, MyOrder.class);
		result.print();
	}
}
