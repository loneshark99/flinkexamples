package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;

public class ForestMonitorData {
    public final static String TEMPERATURE = "TEMPERATURE";
    public final static String SMOKE = "SMOKE";
    public final static List<String> SMOKE_WARNING = Arrays.asList("HIGH", "MIDDLE");
    private String type;
    private double temperature;
    private String smoke;

    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
    public double getTemperature() {
        return temperature;
    }
    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }
    public String getSmoke() {
        return smoke;
    }
    public void setSmoke(String smoke) {
        this.smoke = smoke;
    }

    @Override
    public String toString() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "Error converting to JSON: " + e.getMessage();
        }
    }
}
