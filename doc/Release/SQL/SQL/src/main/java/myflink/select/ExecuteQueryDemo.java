package myflink.select;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class ExecuteQueryDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
// enable checkpointing
        tableEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tableEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        String contents = "" +
                "1,BMW,3,2019-12-12 00:00:01\n" +
                "1,BMW,3,2019-12-12 00:00:01\n" +
                "1,BMW,3,2019-12-12 00:00:01\n" +
                "1,BMW,3,2019-12-12 00:00:01\n" +
                "1,BMW,3,2019-12-12 00:00:01\n" +
                "1,BMW,3,2019-12-12 00:00:11\n" +
                "1,BMW,3,2019-12-12 00:00:01\n" +
                "1,BMW,3,2019-12-12 00:00:01\n" +
                "1,BMW,3,2019-12-12 00:00:01\n" +
                "1,BMW,3,2019-12-12 00:10:01\n" +

                "2,Tesla,4,2019-12-12 00:00:02\n";
        String path = createTempFile(contents);
        tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT)  WITH ('connector.type' = 'filesystem','connector.path' = 'path','format.type' = 'csv')");

// execute SELECT statement
        TableResult tableResult1 = tableEnv.executeSql("SELECT * FROM Orders");
// use try-with-resources statement to make sure the iterator will be closed automatically
        try (CloseableIterator<Row> it = tableResult1.collect()) {
            while(it.hasNext()) {
                Row row = it.next();
                // handle row
            }
        }

// execute Table
        TableResult tableResult2 = tableEnv.sqlQuery("SELECT * FROM Orders").execute();
        tableResult2.print();
    }
    /**
     * 用contents创建一个临时文件并返回绝对路径。
     */
    private static String createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("orders", ".csv");
        tempFile.deleteOnExit();
        FileUtils.writeFileUtf8(tempFile, contents);
        return tempFile.toURI().toString();
    }
}
