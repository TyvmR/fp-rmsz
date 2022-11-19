package cn.flink.opt1;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkJson2HDFSCsv {

    public static void main(String[] args) {
        //获取tableEnvironment

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                // .inStreamingMode()//默认就是这个
                //      .useBlinkPlanner()//flink1.14版本之后，默认就是使用了Blink
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);


        String source_sql = "CREATE TABLE json_table (\n" +
                "  id Integer,\n" +
                "  name STRING,\n" +
                "  email STRING,\n" +
                "  date_time STRING" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='input/userbase.json',\n" +
                "  'format'='json'\n" +
                ")";

        String sink_sql = "CREATE TABLE sink_hdfs (\n" +
                "  id Integer,\n" +
                "  name STRING,\n" +
                "  email STRING,\n" +
                "  date_time STRING" +
                ") WITH ( \n " +
                " 'connector' = 'filesystem',\n" +
                " 'path' = 'hdfs://bigdata01:8020/output_csv/' , \n" +
                " 'format' = 'csv'\n" +
                ")";


        String insert_sql = "insert into sink_hdfs select id,name,date_time,email from json_table ";

        tEnv.executeSql(source_sql);
        tEnv.executeSql(sink_sql);
        tEnv.executeSql(insert_sql);

    }

}
