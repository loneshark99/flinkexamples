package com.alexwang.flink.model;

import lombok.Builder;
import lombok.Data;

import java.util.Arrays;
import java.util.List;

@Data
@Builder
public class ForestMonitorData {
    public final static String TEMPERATURE = "TEMPERATURE";
    public final static String SMOKE = "SMOKE";
    public final static List<String> SMOKE_WARNING = Arrays.asList("HIGH", "MIDDLE");
    private String type;
    private double temperature;
    private String smoke;
}
