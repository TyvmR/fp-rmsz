package org.sword.chapter11_1;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.sword.Event;

import static org.apache.flink.table.api.Expressions.e;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @Author: john
 * @Date: 2023-01-03-10:23
 * @Description:
 */
public class Chapter11_1 {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT,19999);
        StreamExecutionEnvironment localEnvironmentWithWebUI = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        DataStreamSource<Event> eventDataStreamSource = localEnvironmentWithWebUI.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L)
        );
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(localEnvironmentWithWebUI);

        Table table = streamTableEnvironment.fromDataStream(eventDataStreamSource);
        Table table1 = streamTableEnvironment.sqlQuery("select url, user from " + table);
        streamTableEnvironment.toDataStream(table1).print();
        streamTableEnvironment.execute("");
    }


    public static void commonTenv(){
        EnvironmentSettings build = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(build);
        Table table = tableEnvironment.fromValues(DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                DataTypes.FIELD("name", DataTypes.STRING())
        ), row(1, "ABC"), row(2L, "ABCD"));
        tableEnvironment.createTemporaryView("row_table",table);
        TableResult tableResult = tableEnvironment.executeSql("select * from row_table");
        tableResult.print();
    }
}
