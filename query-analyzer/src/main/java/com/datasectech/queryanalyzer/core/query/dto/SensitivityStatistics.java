package com.datasectech.queryanalyzer.core.query.dto;

import java.util.Map;

public class SensitivityStatistics {

    public final double selectivity;
    public final int estimatedRows;
    public final Map<String, String> sensitiveColumns;

    public SensitivityStatistics(double selectivity, int estimatedRows, Map<String, String> sensitiveColumns) {
        this.selectivity = selectivity;
        this.estimatedRows = estimatedRows;
        this.sensitiveColumns = sensitiveColumns;
    }

    @Override
    public String toString() {
        return "SensitivityStatistics{" +
                "selectivity=" + selectivity +
                ", estimatedRows=" + estimatedRows +
                ", sensitiveColumns=" + sensitiveColumns +
                '}';
    }
}
