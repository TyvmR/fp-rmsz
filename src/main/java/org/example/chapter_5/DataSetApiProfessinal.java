package org.example.chapter_5;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.java.aggregation.Aggregations.MIN;
import static org.apache.flink.api.java.aggregation.Aggregations.SUM;


/**
 * @Author: john
 * @Date: 2022-10-08-20:32
 * @Description: dataset api专用算子
 */
public class DataSetApiProfessinal {

    public static void main(String[] args) throws Exception {
//        aggregateOnGroupedDataSetTuple();
//          orderByTransformationOpr();
//        mapPartitionCounterOpr();
//            pojoGroupBy();
            firstNElenOpr();
    }



    /*
     * @desc: 聚合元组
     * @author Administrator
     *
     * @date 2022/10/8 20:45
     * @param ``
     * @return
     * @version 1.0.0
     */
    public static void aggregateOnGroupedDataSetTuple() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple3> dataSource = env.fromElements(
                new Tuple3(1, "a", 1.0),
                new Tuple3(2, "b", 2.0),
                new Tuple3(4, "b", 4.0),
                new Tuple3(3, "c", 3.0)

        );
        AggregateOperator<Tuple3> output1 = dataSource
                .groupBy(1)
                .aggregate(SUM, 0)
                .and(MIN, 2);
        AggregateOperator<Tuple3> output2 = dataSource
                .groupBy(1)
                .aggregate(SUM, 0)
                .aggregate(MIN, 2);

        output1.print();
        System.out.println("==========");
        output2.print();
    }




    /*
     * @desc: 排序转换
     * @author Administrator
     * @date 2022/10/8 21:25
     * @param ``
     * @return
     * @version 1.0.0
     */
    public static void orderByTransformationOpr() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple3> dataSource = env.fromElements(
                new Tuple3(1, "a", 1.0),
                new Tuple3(2, "b", 2.0),
                new Tuple3(4, "b", 4.0),
                new Tuple3(5, "b", 1.0),
                new Tuple3(3, "c", 3.0)

        );
        dataSource.groupBy(1)
                .minBy(0,2)
                .print();

    }


    /*
     * @desc: 分区转换计数
     * @author Administrator
     * @date 2022/10/8 22:14
     * @param ``
     * @return
     * @version 1.0.0
     */
    public static void mapPartitionCounterOpr() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.fromElements(
              "BMW",
                "Tesla",
                "Rolls-Royce"
        );
        dataSource.mapPartition(new MapPartitionFunction<String, Long>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<Long> collector) throws Exception {
                long i = 0;
                for (String s : iterable) {
                    i++;
//                    System.out.println(i);
                }
//                System.out.println(i);
                collector.collect(i);
            }
        }).print();
    }


    public static void pojoGroupBy() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<WC> dataSource = env.fromElements(
                new WC("BMW", 1),
                new WC("Tesla", 1),
                new WC("Tesla", 9),
                new WC("Rolls-Royce", 1)
        );
//        dataSource.groupBy("word").reduce(new WordCounter()).print();
//        dataSource.reduce(new WordCounter()).print();

        //使用键选择器
        dataSource.groupBy(new KeySelector<WC, String>() {
            @Override
            public String getKey(WC wc) throws Exception {
                return wc.word;
            }
        }).reduce(new WordCounter()).print();
    }


    public static class WC {


        public WC() {
        }

        public String word;
        public int count;

        public WC(String word,int count){
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


    public static class WordCounter implements ReduceFunction<WC> {

        @Override
        public WC reduce(WC wc, WC t1) throws Exception {
            return new WC(wc.word,wc.count + t1.count);
        }
    }


    //first-n元素
    public static void firstNElenOpr() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<String, Integer>> dataSource = environment.fromElements(
                Tuple2.of("BMW", 30),
                Tuple2.of("Tesla", 30),
                Tuple2.of("Tesla", 35),
                Tuple2.of("Tesla", 55),
                Tuple2.of("Rolls-Royce", 300),
                Tuple2.of("BMW", 40),
                Tuple2.of("BMW", 45),
                Tuple2.of("BMW", 80)
        );
        GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> out1 = dataSource
                .first(2);
        GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> out2 = dataSource
                .groupBy(0)
                .first(2);
        GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> out3 = dataSource
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .first(2);
        out1.print();
        System.out.println("==========");
        out2.print();
        System.out.println("==========");
        out3.print();
    }

}
