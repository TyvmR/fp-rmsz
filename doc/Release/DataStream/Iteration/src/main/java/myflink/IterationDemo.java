package myflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class IterationDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> input = env.generateSequence(0,4);
        //使用iterate()方法创建迭代流，iterate()方法如果有参数，则允许用户指定等待反馈的下一个输入元素的最大时间间隔
        IterativeStream<Long> iterativeStream = input.iterate();
        //增加处理逻辑，对元素执行减一操作
        DataStream<Long>  zero = iterativeStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1 ;
            }
        });
        //获取要进行迭代的流
        DataStream<Long> stillGreaterThanZero = zero.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });
        //对需要迭代的流形成一个闭环，设置feedback，这个数据流是被反馈的通道，只要是value>0的数据都会被重新迭代计算
        iterativeStream.closeWith(stillGreaterThanZero);
        //小于等于0的数据继续向前传输
        DataStream<Long> lessThanZero = zero.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value <= 0);
            }
        });
        zero.print("IterationDemo");

        env.execute();
    }


}
