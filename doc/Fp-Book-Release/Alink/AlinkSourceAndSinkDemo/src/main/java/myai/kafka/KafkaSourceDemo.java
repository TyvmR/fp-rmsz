package myai.kafka;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.operator.stream.sink.Kafka011SinkStreamOp;
import com.alibaba.alink.operator.stream.source.Kafka011SourceStreamOp;


/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        /*
         *输入的数据格式：{"id":2,"clicks":15}
         */
        Kafka011SourceStreamOp source =
                new Kafka011SourceStreamOp()
                        //Kafka的服务器地址
                        .setBootstrapServers("localhost:9092")
                        //订阅的Kafka主题
                        .setTopic("mytopic")
                        //开始模式：支持"EARLIEST","GROUP_OFFSETS","LATEST","TIMESTAMP"
                        .setStartupMode("LATEST")
                        //设置分组id
                        .setGroupId("alink_group");
        //输出数据
        source.print();
/**
 * 使用JsonValueStreamOp对字符串的数据进行提取。设置需要提取内容的JsonPath，提取出各列数据
 */
        StreamOperator data = source
                .link(
                        new JsonValueStreamOp()
                                .setSelectedCol("message")
                                .setReservedCols(new String[]{})
                                .setOutputCols(
                                        new String[]{"id", "clicks"})
                                .setJsonPath(new String[]{"$.id", "$.clicks"})
                );
        //输出数据的Schema
        System.out.print(data.getSchema());

        data.print();

        Kafka011SinkStreamOp sink = new Kafka011SinkStreamOp()
                .setBootstrapServers("localhost:9092").setDataFormat("json")
                .setTopic("mytopic");
        sink.linkFrom(data);

        StreamOperator.execute();
    }
}
