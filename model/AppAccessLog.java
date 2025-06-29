package com.alexwang.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AppAccessLog {
    private String sessionID;
    private String uri;
    private long timestamp;
}
