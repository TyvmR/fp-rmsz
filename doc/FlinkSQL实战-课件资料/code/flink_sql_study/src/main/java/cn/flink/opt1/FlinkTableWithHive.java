package cn.flink.opt1;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;

public class FlinkTableWithHive {

    public static void main(String[] args) {

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .inBatchMode()
                // .inStreamingMode()//默认就是这个
                //      .useBlinkPlanner()//flink1.14版本之后，默认就是使用了Blink
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);
        String  name ="myCataLog";
        String defaultDatabase = "test";
        String hiveConfDir = "input/hiveconf/";

        //设置需要加载hive的模块
        tEnv.loadModule(name,new HiveModule("3.1.2"));
        //设置运行的方言
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        //注册一个catalog  类似于一个元数据的管理中心
        tEnv.registerCatalog(name,hiveCatalog);

        //使用catalog以及对应的数据库
        tEnv.useCatalog(name);
        tEnv.useDatabase(defaultDatabase);
        tEnv.executeSql("insert into user_count select name as username,count(1) as count_result from userbase group by name");
    }

}
