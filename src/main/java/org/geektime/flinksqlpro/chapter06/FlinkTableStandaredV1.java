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
