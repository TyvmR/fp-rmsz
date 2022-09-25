package myflink.dataset.demo14;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class ReduceonDataSetGroupedbyKeySelectorFunction {
    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<WC> words = env.fromElements(
                new WC("BMW", 1),
                new WC("Tesla", 1),
                new WC("Tesla", 9),
                new WC("Rolls-Royce", 1));

        DataSet<WC> wordCounts = words
                        // DataSet grouping on field "word"
                        .groupBy(new SelectWord())
                        // apply ReduceFunction on grouped DataSet
                        .reduce(new WordCounter());

        wordCounts.print();

    }
    // some ordinary POJO
    public static class WC {
        public String word;
        public int count;

        public WC() {
        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
    public static class SelectWord implements KeySelector<WC, String> {

        @Override
        public String getKey(WC value) throws Exception {
            return value.word;
        }
    }

    //ReduceFunction that sums Integer attributes of a POJO
    public static class WordCounter implements ReduceFunction<WC> {
        @Override
        public WC reduce(WC in1, WC in2) {
            return new WC(in1.word, in1.count + in2.count);
        }
    }
}
