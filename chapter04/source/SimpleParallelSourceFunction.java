package com.alexwang.flink.chapter04.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleParallelSourceFunction extends RichParallelSourceFunction<Long> {

    private final static AtomicBoolean running = new AtomicBoolean(true);

    private final long from;
    private final long end;

    public SimpleParallelSourceFunction(long from, long end) {
        this.from = from;
        this.end = end;
    }

    public SimpleParallelSourceFunction(long from) {
        this(from, Long.MAX_VALUE);
    }


    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        int total = getRuntimeContext().getNumberOfParallelSubtasks();
        int current = getRuntimeContext().getIndexOfThisSubtask();

        long totalNum = end - from;
        long totalTasks = (totalNum / total) == 0 ? 1 : (totalNum / total);
        long start = this.from + (totalTasks * current);
        if (start >= this.end)
            return;

        long to = this.from + totalTasks * (current + 1);
        if (current == total - 1)
            to = this.end;

        for (long i = start; i < to && running.get(); i++) {
            ctx.collect(i);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        running.set(false);
    }
}
