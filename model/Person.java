package com.alexwang.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"id", "name", "address", "age"})
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Person {
    private int id;
    private String name;
    private String address;
    private int age;
}
