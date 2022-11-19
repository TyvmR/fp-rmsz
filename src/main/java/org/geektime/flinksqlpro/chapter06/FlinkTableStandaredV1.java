package org.geektime.flinksqlpro.chapter06;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @Author: john
 * @Date: 2022-11-19-13:04
 * @Description:
 */
public class FlinkTableStandaredV1 {



    public static void main(String[] args) {

//        localToHdfs();
        localToKafka();

    }



    public static void localToKafka() {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        String source_sql = "CREATE TABLE json_table (\n" +
                "  id Integer,\n" +
                "  name STRING,\n" +
                "  email STRING,\n" +
                "  date_time STRING" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='doc/userbase.json',\n" +
                "  'format'='json'\n" +
                ")";

        String sink_table = "CREATE TABLE kafka_table (\n" +
                "  `id` Integer,\n" +
                "  `name` STRING,\n" +
                "  `email` STRING,\n" +
                "  `date_time` STRING \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'john_test',\n" +
                "  'properties.bootstrap.servers' = '10.0.12.240:18108,10.0.12.252:18108,10.0.12.253:18108',\n" +
                "  'format' = 'json'\n" +
                ")";

        String insert_sql = "insert into kafka_table select id,name,date_time,email from json_table ";
        tableEnvironment.executeSql(source_sql);
        tableEnvironment.executeSql(sink_table);
        tableEnvironment.executeSql(insert_sql);
    }



    public static void localToHdfs() {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String source_sql = "CREATE TABLE json_table (\n" +
                "  id Integer,\n" +
                "  name STRING,\n" +
                "  email STRING,\n" +
                "  date_time STRING" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='doc/userbase.json',\n" +
                "  'format'='json'\n" +
                ")";

        String sink_sql = "CREATE TABLE sink_hdfs (\n" +
                "  id Integer,\n" +
                "  name STRING,\n" +
                "  email STRING,\n" +
                "  date_time STRING" +
                ") WITH ( \n " +
                " 'connector' = 'filesystem',\n" +
                " 'path' = 'hdfs://mycluster/dodb/output-csv/' , \n" +
                " 'format' = 'csv'\n" +
                ")";

        String insert_sql = "insert into sink_hdfs select id,name,date_time,email from json_table ";
        tableEnvironment.executeSql(source_sql);
        tableEnvironment.executeSql(sink_sql);
        tableEnvironment.executeSql(insert_sql);
    }

    public static void printCommon() {
        //环境
        EnvironmentSettings build = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(build);
        //获取表
        Table initTab = tableEnvironment.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id",DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING())),
                row(1, "zhangsan"),
                row(2, "lisi")
        ).select($("id"), $("name"));

        //注册表
        tableEnvironment.createTemporaryView("sourceTable",initTab);
        tableEnvironment.sqlQuery("select * from sourceTable").execute().print();
    }

}
