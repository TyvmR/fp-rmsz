package myai;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MySqlSourceStreamOp;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class My {
    public static void main(String[] args) throws Exception {
        MySqlSourceStreamOp data = new MySqlSourceStreamOp()
                .setDbName("flink")
                .setUsername("long")
                .setPassword("long")
                .setIp("localhost")
                .setPort("3306")
                .setInputTableName("test");
        data.print();
        StreamOperator.execute();
    }


}
