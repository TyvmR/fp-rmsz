package myflink.datastream;

import myflink.source.MySource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class StreamWindowDemo {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取自定义的数据流
        DataStream<Tuple2<String, Integer>> dataStream = env.addSource(new MySource())
                .flatMap(new Splitter())
                .keyBy(0)

                //指定计算数据的窗口大小和滑动窗口大小,即窗口时间内单词计数
                .timeWindow(Time.seconds(10))
                .sum(1);
        //打印数据到控制台
        dataStream.print();
        //执行任务操作。因为flink是懒加载的，所以必须调用execute方法才会执行
        env.execute("WordCount");

    }

    //使用FlatMapFunction函数分割字符串
    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
