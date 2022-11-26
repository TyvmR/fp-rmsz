package org.example.metric;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

/**
 * @Author: john
 * @Date: 2022-11-23-15:05
 * @Description:
 */
public class MetricsMapFunc extends RichMapFunction<Integer, Integer> {

    private static final long serialVersionUID = 1L;

    private transient Counter eventCounter;
    private transient Histogram valueHistogram;


    @Override
    public void open(Configuration parameters) throws Exception {
        eventCounter = getRuntimeContext().getMetricGroup().counter("events");
        valueHistogram =
                getRuntimeContext()
                        .getMetricGroup()
                        .histogram("value_histogram", new DescriptiveStatisticsHistogram(10));
    }

    @Override
    public Integer map(Integer value) throws Exception {
        eventCounter.inc();
        if(eventCounter.getCount() > 100){
            throw new RuntimeException();
        }
        valueHistogram.update(value);
        return value;
    }
}
