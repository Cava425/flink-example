package org.example.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class CustomStringSource implements SourceFunction<String> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRunning) {
            // 产生要发送的字符串
            String message = "Hello, world!";

            // 发送字符串到下游算子
            sourceContext.collect(message);

            // 每隔1秒发送一次
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

