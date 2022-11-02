package org.example.fllikpartbook.chapter_8;

import java.util.List;
import java.util.Map;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @Author: john
 * @Date: 2022-10-15-10:51
 * @Description:
 */
public class CepStreamDemoV1 {


    public static void main(String[] args) throws Exception {

        singleModel();

    }





    /*
     * @desc: 单个模式
     * @author Administrator
     * @date 2022/10/15 11:01
     * @param ``
     * @return
     * @version 1.0.0
     */
    public static void singleModel() throws Exception {
        StreamExecutionEnvironment
                env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.fromElements("a1", "c", "b4", "a2", "b2", "a3");
        Pattern<String, String> pattern = Pattern.<String>begin("start")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {

                        return s.startsWith("a");
                    }
                });
        PatternStream<String> patternStream = CEP.pattern(dataStreamSource, pattern);

        patternStream.process(new PatternProcessFunction<String, String>() {
            @Override
            public void processMatch(Map<String, List<String>> map, Context context, Collector<String> collector) throws Exception {
                System.out.println(map);
            }
        }).print();
        env.execute();
    }
    
    
    
    /*
     * @desc: 循环模式
     * @author Administrator
     * @date 2022/10/15 11:00
     * @param ``
     * @return
     * @version 1.0.0
     */
    public static void circleModel(){

    }


    /*
     * @desc: 组合模式
     * @author Administrator
     * @date 2022/10/15 11:00
     * @param ``
     * @return
     * @version 1.0.0
     */
    public static void consistModel(){

    }
}
