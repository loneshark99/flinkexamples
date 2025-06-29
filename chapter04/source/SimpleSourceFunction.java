package com.alexwang.flink.chapter04.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleSourceFunction extends RichSourceFunction<Long> {

    private final static AtomicBoolean running = new AtomicBoolean(true);

    @Override
    public void open(Configuration parameters) throws Exception {
        int subtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int indexSubTask = getRuntimeContext().getIndexOfThisSubtask();
        System.out.println("total tasks: " + subtasks + ", current task index:" + indexSubTask);
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (running.get()) {
            final long value = ThreadLocalRandom.current().nextLong(2000);
            ctx.collect(value);
            TimeUnit.MILLISECONDS.sleep(value);
        }
    }

    @Override
    public void cancel() {
        running.set(false);
    }
}
