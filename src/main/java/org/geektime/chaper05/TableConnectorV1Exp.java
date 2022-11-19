package org.geektime.chaper05;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author: john
 * @Date: 2022-11-09-11:07
 * @Description:
 */
public class TableConnectorV1Exp {


    public static void main(String[] args) {
        EnvironmentSettings build = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(build);
//        tableEnvironment.
    }
}
