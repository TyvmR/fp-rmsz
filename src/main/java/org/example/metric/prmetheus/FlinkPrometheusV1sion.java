package org.example.metric.prmetheus;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.example.metric.MetricsMapFunc;
import org.example.metric.RandomSourceFunc;

/**
 * @Author: john
 * @Date: 2022-11-23-15:02
 * @Description:
 */
public class FlinkPrometheusV1sion {

    public static void main(String[] args) {

        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT,9998);
        config.setString("metrics.reporter.prom.class","org.apache.flink.metrics.prometheus.PrometheusReporter");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.disableOperatorChaining();
        env.setParallelism(1);
        DataStreamSource<Integer> source= env.addSource(new RandomSourceFunc(Integer.MAX_VALUE));
        source.print();
        source.map(new MetricsMapFunc())
                .name(MetricsMapFunc.class.getSimpleName())
                .addSink(new DiscardingSink<>())
                .name(DiscardingSink.class.getSimpleName());
        try {
            env.execute("");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
