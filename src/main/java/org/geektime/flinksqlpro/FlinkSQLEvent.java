package org.geektime.flinksqlpro;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkSQLEvent {
    //使用event_time作为基准时间线
    public static void main(String[] args) {
        //获取tableEnvironment
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                // .inStreamingMode()//默认就是这个
                //      .useBlinkPlanner()//flink1.14版本之后，默认就是使用了Blink
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);
        //2.创建source table,这种方式会自动注册表
        tEnv.executeSql("CREATE TABLE userbase ("+
                " id Integer,"+
                " name STRING,"+
                " email STRING,"+
                " date_time TIMESTAMP(3),"+
                " WATERMARK FOR date_time AS date_time - INTERVAL '10' SECOND"+
                ") WITH ("+
                " 'connector' = 'filesystem',"+
                " 'path' = 'doc/userbase.json',"+
                " 'format' = 'json'"+
                ")");
        tEnv.sqlQuery("select * from userbase").execute().print();



    }
}
