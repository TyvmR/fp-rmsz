package myflink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;


import static org.apache.flink.table.api.Expressions.$;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 *转换DataSet为Table，*应用Group、Aggregate、 Select、 Filter算子
 */
public class WordCountTable {

	public static void main(String[] args) throws Exception {
		//获取执行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);


		DataSet<WC> input = env.fromElements(
				new WC("Hello", 1),
				new WC("Flink", 1),
				new WC("Hello", 1));
		//转换DataSet为Table
		Table table = tEnv.fromDataSet(input);
		//在注册的表上执行SQL查询并把取回的结果作为一个新的Table
		Table filtered = table
				.groupBy($("word"))
				.select($("word"), $("frequency").sum().as("frequency"))
				.filter($("frequency").isEqual(2));

		DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);

		result.print();
	}

}
