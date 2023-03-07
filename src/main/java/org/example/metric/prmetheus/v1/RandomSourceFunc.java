package org.example.metric.prmetheus.v1;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @Author: john
 * @Date: 2022-11-23-15:04
 * @Description:
 */
public class RandomSourceFunc implements SourceFunction<Integer> {


    public RandomSourceFunc(int elements) {
        this.elements = elements;
    }

    private int count = 0;
    private volatile boolean isRunning = true;
    private int elements;


    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (isRunning && count < elements) {
            Thread.sleep(1000);
            sourceContext.collect(ThreadLocalRandom.current().nextInt(10));
            count++;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
