package com.alexwang.flink.chapter08;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

class IntSourceSplit implements SourceSplit, Serializable {

    private final int from;
    private final int until;
    private int currentValue;

    IntSourceSplit(int from, int until, int currentValue) {
        this.from = from;
        this.until = until;
        this.currentValue = currentValue;
    }

    @Override
    public String splitId() {
        return String.format("%d-%d", this.from, this.until);
    }

    public int getFrom() {
        return from;
    }

    public int getUntil() {
        return until;
    }

    public int getCurrentValue() {
        return currentValue;
    }

    public void incCurrentValue() {
        this.currentValue += 1;
    }
}
