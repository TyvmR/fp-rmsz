package myflink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CustomerSource implements SourceFunction<Long> {
   boolean isRunning= true;
    Long num = 0L;
    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning) {
            ctx.collect(num);
            num =num+ 1;
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
