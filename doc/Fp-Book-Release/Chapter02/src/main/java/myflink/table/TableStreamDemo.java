package myflink.table;

import myflink.pojo.MyOrder;
import myflink.source.MySource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 *将数据集转换为表，应用分组，聚合，选择和过滤操作
 */
public class TableStreamDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, bsSettings);
		// or TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);

		//获取自定义的数据流
		DataStream<String> dataStream = sEnv.addSource(new MySource());

		Table table1 = tEnv.fromDataStream(dataStream,$("word"));

		Table table = table1.where($("word").like("%t%"));

		String explantion_old = tEnv.explain(table);

		System.out.println(explantion_old);

		tEnv.toAppendStream(table, Row.class).print("table");

		sEnv.execute();

	}


}
