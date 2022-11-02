package myflink;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class CEPCombiningPatternDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.fromElements("a1", "c","b4" ,"a2", "b2", "a3");

        //定义匹配模式（pattern）,模式序列
        Pattern<String, ?> pattern = Pattern.<String>begin("start").where(new SimpleCondition<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("a");
            }
        })
                // 实现组合模式
                .next("end").where(new SimpleCondition<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("b");
            }
        });


        //执行Pattern
        PatternStream<String> patternStream = CEP.pattern(input, pattern);

        //从Pattern Stream上检出匹配事件序列
        DataStream<String> result = patternStream.process(
                new PatternProcessFunction<String, String>() {
                    @Override
                    public void processMatch(Map<String, List<String>> match, Context ctx, Collector<String> out) throws Exception {
                        System.out.println(match);
                        // out.collect(new String("match"));
                    }

                });

        result.print();
        env.execute();
    }
}
