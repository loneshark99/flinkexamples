package com.alexwang.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CityTemperature {
    @JsonProperty("CITY")
    private String city;
    @JsonProperty("TEMPERATURE")
    private float temperature;
    @JsonProperty("TIMESTAMP")
    private long timestamp;
}