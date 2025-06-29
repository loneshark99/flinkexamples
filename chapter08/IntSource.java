package com.alexwang.flink.chapter08;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class IntSource implements Source<Integer, IntSourceSplit, EnumeratorState> {
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<IntSourceSplit, EnumeratorState> createEnumerator(SplitEnumeratorContext<IntSourceSplit> enumContext) throws Exception {
        return new IntEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<IntSourceSplit, EnumeratorState> restoreEnumerator(SplitEnumeratorContext<IntSourceSplit> enumContext, EnumeratorState checkpoint) throws Exception {
        return new IntEnumerator(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<IntSourceSplit> getSplitSerializer() {
        return new SimpleSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<EnumeratorState> getEnumeratorCheckpointSerializer() {
        return new SimpleSerializer();
    }

    @Override
    public SourceReader<Integer, IntSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new IntRangeReader(readerContext);
    }
}
