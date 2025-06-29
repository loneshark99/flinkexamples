package com.alexwang.flink.chapter08;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class EnumeratorState implements Serializable {

    private int currentValue;

    private List<IntSourceSplit> deadSplits;

    EnumeratorState(int currentValue, List<IntSourceSplit> deadSplits) {
        this.currentValue = currentValue;
        this.deadSplits = deadSplits;
    }

    EnumeratorState(int currentValue) {
        this(currentValue, new ArrayList<>());
    }

    public int getCurrentValue() {
        return currentValue;
    }

    public void setCurrentValue(int currentValue) {
        this.currentValue = currentValue;
    }

    public List<IntSourceSplit> getDeadSplits() {
        return deadSplits;
    }

    public void setDeadSplits(List<IntSourceSplit> deadSplits) {
        this.deadSplits = deadSplits;
    }
}
