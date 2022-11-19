package org.sword;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.$;


/**
 * @Author: john
 * @Date: 2022-11-18-10:43
 * @Description: 练习流表 动态表 flinksql
 */
public class DynamicTableWithStreamV1 {

    private static final Logger logger = LoggerFactory.getLogger(DynamicTableWithStreamV1.class);

    public static void main(String[] args) throws Exception {

    }


    //滚动窗口
    public static void commonWithWindowAndWater() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> singleOutputStreamOperator = env.fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home",  3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        logger.info("time is {}",event.getTimestamp());
                                        return event.getTimestamp();
                                    }
                                })
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将数据流转换成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(
                singleOutputStreamOperator,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
        // 将 timestamp 指定为事件时间，并命名为 ts
        );
        tableEnv.createTemporaryView("EventTable", eventTable);
        Table result = tableEnv
                .sqlQuery(
                        "SELECT " +
                                "user, " +
                                "window_end AS endT, " + // 窗口结束时间
                                "COUNT(url) AS cnt " + // 统计 url 访问次数
                                "FROM TABLE( " +
                                "TUMBLE( TABLE EventTable, " + // 1 小时滚动窗口
                                "DESCRIPTOR(ts), " +
                                "INTERVAL '1' HOUR)) " +
                                "GROUP BY user, window_start, window_end "
                );
        tableEnv.toDataStream(result).print(" watermake:");
        env.execute();
    }


    //不带窗口和水印
    public static void common() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> singleOutputStreamOperator = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                new Event("Bob", "./prod?id=3", 3600 * 1000L + 60 * 1000L),
                new Event("Cary", "./home",  3600 * 1000L + 30 * 60 * 1000L),
                new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
        );
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("EventTable", singleOutputStreamOperator, $("user"), $("url"),
                $("timestamp").as("ts"));
        //更新查询
//        Table urlCountTable = tableEnv.sqlQuery("SELECT user, COUNT(url) as cnt FROM EventTable GROUP BY user");
//        tableEnv.toChangelogStream(urlCountTable).print("count");
        //追加查询
        Table aliceVisitTable = tableEnv.sqlQuery("SELECT url, user FROM EventTable WHERE user = 'Cary'");
        tableEnv.toDataStream(aliceVisitTable).print("count");
        env.execute();
    }
}
