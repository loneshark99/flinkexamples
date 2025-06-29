package com.alexwang.flink.chapter08;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class IntEnumerator implements SplitEnumerator<IntSourceSplit, EnumeratorState> {

    private final SplitEnumeratorContext<IntSourceSplit> context;
    private final EnumeratorState enumeratorState;

    public IntEnumerator(SplitEnumeratorContext<IntSourceSplit> context, EnumeratorState enumeratorState) {
        this.context = context;
        this.enumeratorState = enumeratorState;
    }

    public IntEnumerator(SplitEnumeratorContext<IntSourceSplit> context) {
        this(context, new EnumeratorState(0));
    }

    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!enumeratorState.getDeadSplits().isEmpty()) {
            IntSourceSplit sourceSplit = enumeratorState.getDeadSplits().remove(0);
            this.context.assignSplit(sourceSplit, subtaskId);
        } else {
            int from = this.enumeratorState.getCurrentValue();
            int until = from + 1000;
            this.context.assignSplit(new IntSourceSplit(from, until, from), subtaskId);
            this.enumeratorState.setCurrentValue(until);
        }
    }

    @Override
    public void addSplitsBack(List<IntSourceSplit> splits, int subtaskId) {
        this.enumeratorState.getDeadSplits().addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public EnumeratorState snapshotState(long checkpointId) throws Exception {
        return this.enumeratorState;
    }

    @Override
    public void close() throws IOException {

    }
}
