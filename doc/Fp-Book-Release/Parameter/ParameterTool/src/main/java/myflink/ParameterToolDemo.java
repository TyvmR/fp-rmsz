package myflink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class ParameterToolDemo {
    public static void main(String[] args) throws Exception {
        /*从Map中读取参数*/
        Map properties = new HashMap();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("zookeeper.connect", "172.0.0.1:2181");
        properties.put("topic", "myTopic");

        ParameterTool parameterTool = ParameterTool.fromMap(properties);
        System.out.println(parameterTool.getRequired("topic"));
        System.out.println(parameterTool.getProperties());

        /*从 .properties files文件中读取参数*/
        String propertiesFilePath = "src/main/resources/myjob.properties";
                ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);
        System.out.println(parameter.getProperties());
        System.out.println(parameter.getRequired("my"));

        File propertiesFile = new File(propertiesFilePath);
        ParameterTool parameterFlie = ParameterTool.fromPropertiesFile(propertiesFile);
        System.out.println(parameterFlie.getProperties());
        System.out.println(parameterFlie.getRequired("my"));


        /*从命令行中读取参数*/
        ParameterTool parameterfromArgs = ParameterTool.fromArgs(args);
        System.out.println("parameterfromArgs:" + parameterfromArgs.getProperties());

        /*从系统配置中读取参数*/
        ParameterTool parameterfromSystemProperties = ParameterTool.fromSystemProperties();
        System.out.println("parameterfromSystemProperties" + parameterfromSystemProperties.getProperties());

    }
}
