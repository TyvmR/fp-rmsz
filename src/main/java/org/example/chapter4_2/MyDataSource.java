package org.example.chapter4_2;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Copyright (C), 2021-2021
 * FileName: MyDataSource
 * Author:   Administrator
 * Date:     2021/12/10 9:51
 * Description:
 */

public class MyDataSource implements SourceFunction<Long> {


    private long count  = 0;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
            while (true){
                sourceContext.collect(count);
                count++;
                Thread.sleep(1L);
            }
    }

    @Override
    public void cancel() {

    }
}


