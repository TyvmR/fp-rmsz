package myflink.transformations;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class MyFilter {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //准备数据
        DataSet<Integer> input = env.fromElements(-1,-2,-3,1,2,3,8,417);
        DataSet<Integer> ds = input.filter(new MyFilterFunction());
        ds.print();
    }

    public static class MyFilterFunction extends RichFilterFunction<Integer> {


        @Override
        public boolean filter(Integer value) throws Exception {
            return value>0;
        }
    }
}
