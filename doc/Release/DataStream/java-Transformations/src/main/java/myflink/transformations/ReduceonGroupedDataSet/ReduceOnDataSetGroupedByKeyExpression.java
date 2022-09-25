package myflink.transformations.ReduceonGroupedDataSet;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


public class ReduceOnDataSetGroupedByKeyExpression {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
/*Reduce on DataSet Grouped by Key Expression*/
        DataSet<WC> words = env.fromElements(new WC("词",1),
                new WC("词哦",2));
                DataSet<WC> wordCounts = words
                        // DataSet grouping on field "word"
                        .groupBy("word")
                        // apply ReduceFunction on grouped DataSet
                        .reduce(new WordCounter());
        words.print();


    }
    public static class WC {
        public String word;
        public int count;

        public WC() {
        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
    // ReduceFunction that sums Integer attributes of a POJO
    public static class WordCounter implements ReduceFunction<WC> {
        @Override
        public WC reduce(WC in1, WC in2) {
            return new WC(in1.word, in1.count + in2.count);
        }
    }



}
