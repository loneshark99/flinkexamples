package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleSourceFunction extends RichSourceFunction<Long>
{
    private final static AtomicBoolean running = new AtomicBoolean(true);
    private int subTasks;
    private int indexSubTask;

    @Override
    public void open(Configuration parameters) throws Exception {
        subTasks = this.getRuntimeContext().getNumberOfParallelSubtasks();
        indexSubTask = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while(running.get())
        {
            final long value = ThreadLocalRandom.current().nextLong(2000);
            sourceContext.collect(value);
            System.out.println("Total SubTasks: " + subTasks + ", This SubTask Index: " + indexSubTask);
            TimeUnit.MILLISECONDS.sleep(value);
        }
    }

    @Override
    public void cancel() {
        running.set(false);
    }

}
