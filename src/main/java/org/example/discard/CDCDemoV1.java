package org.example.discard;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: john
 * @Date: 2022-11-21-10:56
 * @Description:
 */
public class CDCDemoV1 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(executionEnvironment);
        String sourceTable = "CREATE TABLE mysql_table (\n" +
                "    db_name STRING METADATA FROM 'database_name' VIRTUAL,\n" +
            "        table_name STRING METADATA  FROM 'table_name' VIRTUAL,\n" +
                "    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,\n" +
                "     id INT,\n" +
                "\t name STRING,\n" +
                "     op_time INT,\n" +
                "     age INT,\n" +
                "     PRIMARY KEY(id) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = '10.0.12.254',\n" +
                "     'port' = '18103',\n" +
                "     'username' = 'Rootmaster',\n" +
                "     'password' = 'Rootmaster@777',\n" +
                "     'database-name' = 'cw_cdc_mysql',\n" +
                "     'table-name' = 'newtable')\n" +
                "  ";

//        String sinkTable = "CREATE TABLE kafka_table (\n" +
//                "    `db_name` STRING ,\n" +
//            "        `table_name` STRING,\n" +
//                "    `operation_ts` TIMESTAMP(3) ,\n" +
//                "  `id` INT,\n" +
//                "  `name` STRING,\n" +
//                "  `op_time` INT,\n" +
//                "  `age` INT,\n" +
//                "  PRIMARY KEY (id) NOT ENFORCED " +
//                ") WITH (\n" +
//                "  'connector' = 'upsert-kafka',\n" +
//                "  'topic' = 'john_test',\n" +
//                "  'properties.bootstrap.servers' = '10.0.12.240:18108,10.0.12.252:18108,10.0.12.253:18108',\n" +
//                "  'properties.group.id' = 'testGroup12',\n" +
//                "  'key.format' = 'json',\n" +
//                "  'value.format' = 'json' " +
//                ")";
        tEnv.executeSql(sourceTable);
//        tEnv.executeSql(sinkTable);
//        tEnv.executeSql(" insert into kafka_table select * from  mysql_table");
        TableResult tableResult = tEnv.executeSql(" select * from  mysql_table");
        tableResult.print();
    }
}
