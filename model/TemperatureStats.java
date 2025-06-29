package com.alexwang.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class TemperatureStats {
    @JsonProperty("CITY")
    private String city;
    @JsonProperty("MAX")
    private float max;
    @JsonProperty("MIN")
    private float min;
    @JsonProperty("AVERAGE")
    private float average;

    public static TemperatureStats build(TemperatureStats that) {
        return TemperatureStats.builder()
                .city(that.getCity())
                .max(that.getMax())
                .min(that.getMin())
                .average(that.getAverage())
                .build();
    }
}