package cn.flink.opt4.udaf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

public class FlinkUdafAggregrate {
    
    public static void main(String[] args) {
        //获取tableEnvironment
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                // .inStreamingMode()//默认就是这个
                //      .useBlinkPlanner()//flink1.14版本之后，默认就是使用了Blink
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);

        String source_sql = "CREATE TABLE source_score  (\n" +
                "  id int,\n" +
                "  name STRING,\n" +
                "  course STRING,\n" +
                "  score Double" +
                ") WITH ( \n " +
                " 'connector' = 'filesystem',\n" +
                " 'path' = 'input/score.csv' , \n" +
                " 'format' = 'csv'\n" +
                ")";

        tEnv.executeSql(source_sql);
        //注册函数
        tEnv.createTemporarySystemFunction("AvgFunc",AvgFunc.class);

        tEnv.executeSql("select course,AvgFunc(score) as avg_score from source_score group by course ")
                .print();
    }
    public static class AvgFunc extends AggregateFunction<Double,AvgAccumulator>{
        @Override
        public Double getValue(AvgAccumulator avgAccumulator) {
            if(avgAccumulator.count == 0){
                return null;
            }else{
                return avgAccumulator.sum / avgAccumulator.count;
            }
        }
        @Override
        public AvgAccumulator createAccumulator() {
            return new AvgAccumulator();
        }
        public void accumulate (AvgAccumulator acc,Double score){
            acc.setSum(acc.sum + score);
            acc.setCount(acc.count + 1 );
        }
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AvgAccumulator{
        public double sum = 0.0;
        public  int count = 0;
    }
}
