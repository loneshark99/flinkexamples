package com.alexwang.flink.chapter08;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class IntRangeReader implements SourceReader<Integer, IntSourceSplit> {

    private final SourceReaderContext sourceReaderContext;

    private CompletableFuture<Void> availability = CompletableFuture.completedFuture(null);

    private IntSourceSplit currentSplit;

    public IntRangeReader(SourceReaderContext sourceReaderContext) {
        this.sourceReaderContext = sourceReaderContext;
    }

    @Override
    public void start() {
        System.out.println("IntRangeReader start() method is called.");
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Integer> output) throws Exception {
        if (currentSplit != null && currentSplit.getCurrentValue() < currentSplit.getUntil()) {
            output.collect(currentSplit.getCurrentValue());
            TimeUnit.SECONDS.sleep(1);
            currentSplit.incCurrentValue();
            return InputStatus.MORE_AVAILABLE;
        } else {
            if (availability.isDone()) {
                availability = new CompletableFuture<>();
                sourceReaderContext.sendSplitRequest();
                ;
                System.out.println("send split request to split enumerator(brain)");
            }
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    @Override
    public List<IntSourceSplit> snapshotState(long checkpointId) {
        return Arrays.<IntSourceSplit>asList(currentSplit);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    @Override
    public void addSplits(List<IntSourceSplit> splits) {
        System.out.println("add splits method is called, and size is:" + splits.size());
        this.currentSplit = splits.get(0);
        this.availability.complete(null);
    }

    @Override
    public void notifyNoMoreSplits() {

    }

    @Override
    public void close() throws Exception {

    }
}
