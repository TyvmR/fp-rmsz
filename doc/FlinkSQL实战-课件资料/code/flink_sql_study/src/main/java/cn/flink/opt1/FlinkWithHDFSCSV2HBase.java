package cn.flink.opt1;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkWithHDFSCSV2HBase {
    public static void main(String[] args) {
        //获取tableEnvironment

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                // .inStreamingMode()//默认就是这个
                //      .useBlinkPlanner()//flink1.14版本之后，默认就是使用了Blink
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);

        String source_sql = "CREATE TABLE source_hdfs (\n" +
                "  id Integer,\n" +
                "  name STRING,\n" +
                "  email STRING,\n" +
                "  date_time STRING" +
                ") WITH ( \n " +
                " 'connector' = 'filesystem',\n" +
                " 'path' = 'hdfs://bigdata01:8020/output_csv/' , \n" +
                " 'format' = 'csv'\n" +
                ")";

        String sink_sql = "CREATE TABLE sink_table (\n" +
                " rowkey Integer,\n" +
                " f1 ROW<name STRING,email STRING,date_time STRING > ,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED \n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'hTable',\n" +
                " 'zookeeper.quorum' = 'bigdata01:2181,bigdata02:2181,bigdata03:2181'\n" +
                ") ";


        String execute_sql = "insert into sink_table select id as rowkey ,ROW(name,email,date_time) from source_hdfs";

        tEnv.executeSql(source_sql);
        tEnv.executeSql(sink_sql);
        tEnv.executeSql(execute_sql);



    }



}
