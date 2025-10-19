package org.example;

import javassist.Loader;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleParallelSourceFunction  extends RichParallelSourceFunction<Long>
{
    private final static AtomicBoolean running = new AtomicBoolean(true);
    private final long from;
    private final long end;

    public SimpleParallelSourceFunction(long from, long end)
    {
        this.from = from;
        this.end = end;
    }

    public SimpleParallelSourceFunction(long from)
    {
        this(from, Long.MAX_VALUE);
    }

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        int total = getRuntimeContext().getNumberOfParallelSubtasks();
        int current = getRuntimeContext().getIndexOfThisSubtask();

        long totalNum = end - from;
        long totalTasks = totalNum / total == 0 ? 1 : (totalNum / total);
        long start = from + (current * totalTasks);
        if (start >= this.end)
            return;

        long to = this.from + totalTasks * (current + 1);
        if (current == total - 1)
            to = this.end;

        for (long i = start; i < to && running.get(); i++) {
            sourceContext.collect(i);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        running.set(false);
    }
}
