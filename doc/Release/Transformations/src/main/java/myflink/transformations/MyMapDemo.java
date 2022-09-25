package myflink.transformations;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class MyMapDemo {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置为1
        sEnv.setParallelism(1);
        //准备数据
        DataStream<Integer> dataStream = sEnv.fromElements(4, 1, 7)
                .map(x -> x + 8);
        //输出
        dataStream.print("Map");
        sEnv.execute("Map Job");
    }

}


