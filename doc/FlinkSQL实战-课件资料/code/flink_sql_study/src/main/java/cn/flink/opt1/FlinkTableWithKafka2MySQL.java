package cn.flink.opt1;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkTableWithKafka2MySQL {
    public static void main(String[] args) {

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                // .inStreamingMode()//默认就是这个
                //      .useBlinkPlanner()//flink1.14版本之后，默认就是使用了Blink
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);


        String source_table = "CREATE TABLE KafkaTable (\n" +
                "  `id` Integer,\n" +
                "  `name` STRING,\n" +
                "  `email` STRING,\n" +
                "  `date_time` STRING \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'usr_opt',\n" +
                "  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',\n" +
                "  'properties.group.id' = 'user_opt_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "   'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'true'\n" +
                ")";


        String sink_sql = "CREATE TABLE mysql_sink (\n" +
                "  id Integer,\n" +
                "  username STRING,\n" +
                "  email STRING,\n" +
                "  date_time STRING" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://bigdata03:3306/user_log?characterEncoding=utf-8&serverTimezone=GMT%2B8',\n" +
                "  'driver' = 'com.mysql.jdbc.Driver',\n" +
                "  'table-name' = 'clicklog',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456'\n" +
                ")";

        String execute_sql ="insert into mysql_sink select id , name as username,email,date_time from KafkaTable";

        tEnv.executeSql(source_table);
        tEnv.executeSql(sink_sql);
        tEnv.executeSql(execute_sql);





    }


}
