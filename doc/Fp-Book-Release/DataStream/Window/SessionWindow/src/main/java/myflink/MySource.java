package myflink;


import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class MySource implements SourceFunction<String> {

    private long count = 1L;
    private boolean isRunning = true;

    /**
     * 在run方法中，实现一个循环来产生数据
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {


        while (isRunning) {
            //Word流
            List<String> stringList = new ArrayList<>();
            stringList.add("world");
            stringList.add("Flink");
            stringList.add("Steam");
            stringList.add("Batch");
            stringList.add("Table");
            stringList.add("SQL");
            stringList.add("hello");
            int size=stringList.size();
            int i = new Random().nextInt(size);
            ctx.collect(stringList.get(i));
            System.out.println("Source:"+stringList.get(i));
            //每x（随机）秒产生一条数据
            int rt=i * 1000;
            System.out.println("延迟时间："+rt);
            Thread.sleep(rt);

        }
    }

    //cancel()方法代表取消执行
    @Override
    public void cancel() {
        isRunning = false;
    }
}