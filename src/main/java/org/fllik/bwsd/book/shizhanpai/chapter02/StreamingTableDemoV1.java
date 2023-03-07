package org.fllik.bwsd.book.shizhanpai.chapter02;

import static org.apache.flink.table.api.Expressions.$;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: john
 * @Date: 2022-09-26-13:50
 * @Description:
 */
public class StreamingTableDemoV1 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment, build);
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.addSource(new MySource());
        Table word = streamTableEnvironment.fromDataStream(stringDataStreamSource, $("word"));
        Table word1 = word.where($("word").like("%t%"));
        String explain = streamTableEnvironment.explain(word1);
        System.out.println(explain);
        streamTableEnvironment.toAppendStream(word1, Row.class).print("table");
        executionEnvironment.execute();

    }
}
