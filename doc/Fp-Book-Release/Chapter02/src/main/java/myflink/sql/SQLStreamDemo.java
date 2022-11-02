package myflink.sql;


import myflink.source.MySource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.$;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class SQLStreamDemo {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv=StreamTableEnvironment.create(env);

		DataStream<String> stream = env.addSource(new MySource());

	    //转换DataStream为Table
		Table table = tEnv.fromDataStream(stream, $("word"));
		//查询Table
		Table result = tEnv.sqlQuery("SELECT * FROM "  + table + " WHERE word LIKE '%t%'");

		tEnv.toAppendStream(result, Row.class).print();


		env.execute();
	}

}
