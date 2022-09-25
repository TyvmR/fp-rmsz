package myflink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class ShowDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// show catalogs
        tEnv.executeSql("SHOW CATALOGS").print();
// show databases
        tEnv.executeSql("SHOW DATABASES").print();

// create a table
        tEnv.executeSql("CREATE TABLE my_table (product STRING, amount INT) ");
// show tables
        tEnv.executeSql("SHOW TABLES").print();

// show functions
        tEnv.executeSql("SHOW FUNCTIONS").print();
        String query_expression = "SELECT * FROM my_table";

    }
}
