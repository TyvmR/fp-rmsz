package org.example.chapter02;

import static org.apache.flink.table.api.Expressions.$;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlStreamDemoV1 {

    public static void main(String[] args) throws Exception {
        //1.准备大环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.准备小环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //3.大环境添加source
        DataStreamSource<String> stringDataStreamSource = env.addSource(new MySource());
        //4.转换为小环境的概念
        Table table = tenv.fromDataStream(stringDataStreamSource, $("word"));
        //使用小环境跑sql
        Table result = tenv.sqlQuery("Select * from " + table + " WHERE word like '%t%'");
        tenv.toAppendStream(result, Row.class)
                .print();
        //大环境执行任务
        System.out.println(env.getExecutionPlan());

        env.execute();
    }
}
