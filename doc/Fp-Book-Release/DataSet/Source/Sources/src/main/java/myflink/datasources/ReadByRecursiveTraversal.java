package myflink.datasources;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
/*
* flink支持对一个文件目录内的所有文件，包括所有子目录中的所有文件的遍历访问方式。
* 对于从文件中读取数据，当读取的数个文件夹的时候，嵌套的文件默认是不会被读取的，只会读取第一个文件，其他的都会被忽略。
* 需要使用recursive.file.enumeration进行递归读取
*/
public class ReadByRecursiveTraversal {
    public static void main(String[] args) throws Exception {
        // enable recursive enumeration of nested input files
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // create a configuration object
        Configuration parameters = new Configuration();
        // 设置递归枚举参数,即开启递归
        // set the recursive enumeration parameter
        parameters.setBoolean("recursive.file.enumeration", true);
        // pass the configuration to the data source
        DataSet<String> logs = env.readTextFile("F:\\flink\\Code\\recursive")
                .withParameters(parameters);
        logs.print();
    }
}
