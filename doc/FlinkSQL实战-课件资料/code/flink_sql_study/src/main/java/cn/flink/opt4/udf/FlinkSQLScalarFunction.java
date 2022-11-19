package cn.flink.opt4.udf;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkSQLScalarFunction {

    public static void main(String[] args) {

        //获取tableEnvironment

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                // .inStreamingMode()//默认就是这个
                //      .useBlinkPlanner()//flink1.14版本之后，默认就是使用了Blink
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);


        String source_sql = "CREATE TABLE json_table (\n" +
                "  line STRING \n" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='input/user.json',\n" +
                "  'format'='raw'\n" +  //定义数据内容为raw，表示一整行数据作为一条
                ")";

        tEnv.executeSql(source_sql);

        tEnv.sqlQuery("select * from  json_table ").execute().print();
        //使用自定义的函数，来解析json
        //注册函数
        tEnv.createTemporarySystemFunction("JsonParse",JsonParseFunction.class);

        tEnv.sqlQuery("select JsonParse(line,'date_time'),JsonParse(line,'emial'),JsonParse(line,'id'),JsonParse(line,'name') from json_table")
                .execute().print();


    }

}
