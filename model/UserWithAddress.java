package com.alexwang.flink.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class UserWithAddress implements Serializable {
    private User user;
    private Address address;
}