package cn.flink.opt1;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkTableWithHBase2Kafka {
    public static void main(String[] args) {


        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                // .inStreamingMode()//默认就是这个
                //      .useBlinkPlanner()//flink1.14版本之后，默认就是使用了Blink
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);



        String source_table = "CREATE TABLE opt_log (\n" +
                " rowkey Integer,\n" +
                " f1 ROW<username STRING,email STRING,date_time STRING > ,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED \n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'opt_log',\n" +
                " 'zookeeper.quorum' = 'bigdata01:2181,bigdata02:2181,bigdata03:2181'\n" +
                ") ";



        String sink_table = "CREATE TABLE KafkaTable (\n" +
                "  `username` STRING,\n" +
                "  `email` STRING,\n" +
                "  `date_time` STRING \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_output',\n" +
                "  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',\n" +
                "  'format' = 'json'\n" +
                ")";

        String execute_sql = "insert into KafkaTable select username,email,date_time from opt_log";

        tEnv.executeSql(source_table);
        tEnv.executeSql(sink_table);
        tEnv.executeSql(execute_sql);






    }

}
