package myflink.datasources.ReadCSV;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class ReadCSV {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<User>  inputData = env.readCsvFile("F:\\flink\\Code\\File\\data.csv")
                //fieldDelimiter设置分隔符，默认的是","
                .fieldDelimiter(",")
                //忽略第一行
                .ignoreFirstLine()
                //设置选取哪几列，这里是第2列不选取的。
                .includeFields(true, false, true)
                //pojoType和后面字段名，就是对应列。字段名是不需要对应原始文件header字段名，
                //但必须与POJO里的字段名一一对应
                .pojoType(User.class,"name","age");
        inputData.print();
    }


}
