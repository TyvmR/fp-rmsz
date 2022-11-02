package myflink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class SideOutputDemo {


    public static void main(String[] args) throws Exception {
        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};
        final OutputTag<String> outputTag2 = new OutputTag<String>("side-output2"){};

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> input =env.fromElements(1,2,3,4);

        SingleOutputStreamOperator<Integer> mainDataStream = input
                .process(new ProcessFunction<Integer, Integer>() {

                    @Override
                    public void processElement(
                            Integer value,
                            Context ctx,
                            Collector<Integer> out) throws Exception {

                        out.collect(value);


                        ctx.output(outputTag, "sideout-" + String.valueOf(value));
                        ctx.output(outputTag2, "sideout2-" + String.valueOf(value*3));
                    }
                });

        DataStream<String> sideOutputStream = mainDataStream.getSideOutput(outputTag);
        DataStream<String> sideOutputStream2 = mainDataStream.getSideOutput(outputTag2);
        sideOutputStream.print("sideOutputStream");
        sideOutputStream2.print("sideOutputStream2");
        env.execute();
    }

}