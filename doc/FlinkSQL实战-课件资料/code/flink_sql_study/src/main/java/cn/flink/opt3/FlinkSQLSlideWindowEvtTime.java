package cn.flink.opt3;

import cn.flink.bean.Userproduct;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQLSlideWindowEvtTime {

    public static void main(String[] args) {
        //从socket里面去接收数据，通过watermark来处理乱序的数据

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        //构建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(senv);

        WatermarkStrategy<Userproduct> watermarkStrategy = WatermarkStrategy.<Userproduct>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Userproduct>() {
                    @Override
                    public long extractTimestamp(Userproduct userproduct, long l) {
                        return userproduct.getDate_time() * 1000;
                    }
                });


        //读取数据，指定水位线
        DataStream<Userproduct> userProductDataStream = senv.socketTextStream("bigdata01", 9999)
                .map(event -> {
                    String[] arr = event.split(",");
                    Userproduct userproduct = Userproduct.builder()
                            .product_id(Integer.parseInt(arr[0]))
                            .buyer_name(arr[1])
                            .date_time(Long.valueOf(arr[2]))
                            .price(Double.valueOf(arr[3]))
                            .build();
                    return userproduct;
                }).assignTimestampsAndWatermarks(watermarkStrategy);

        //将流式数据给转换成为动态表
        Table table = tEnv.fromDataStream(userProductDataStream,
                $("product_id"),
                $("buyer_name"),
                $("price"),
                $("date_time").rowtime());//通过调用rowtime来指定event_time为准


        Table resultTable = tEnv.sqlQuery("select product_id,max(price),HOP_START(date_time,INTERVAL '2' second,INTERVAL '4' second) as winstart  from " + table + " group by product_id ,HOP(date_time,INTERVAL '2' second,INTERVAL '4' second)");

        resultTable.execute().print();




    }


}
