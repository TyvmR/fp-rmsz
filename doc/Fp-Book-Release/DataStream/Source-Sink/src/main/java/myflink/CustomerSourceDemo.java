package myflink;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CustomerSourceDemo {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource dataStreamSource = env.addSource(new CustomerSource());

        DataStream<Long> map = dataStreamSource.map(new MyMap()).timeWindowAll(Time.seconds(3))  //统计最近3秒内的数据
                .sum(0);
        map.print();
        env.execute("CustomerSourceDemo");
    }


    private static class MyMap implements MapFunction<Long, Long> {
        @Override
        public Long map(Long value) throws Exception {
//            System.out.println(value);
            return value;
        }
    }
}
