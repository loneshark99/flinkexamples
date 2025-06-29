package com.alexwang.flink.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@JsonDeserialize
@JsonSerialize
@Data
public class FlinkUser {
    @JsonProperty("ID")
    private int id;
    @JsonProperty("USERNAME")
    private String username;
    @JsonProperty("AGE")
    private int age;
}
