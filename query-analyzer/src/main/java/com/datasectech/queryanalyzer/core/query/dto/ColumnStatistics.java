package com.datasectech.queryanalyzer.core.query.dto;

public class ColumnStatistics {
    public String name;
    public boolean isPii;

    public String dataType;
    public int notNull;
    public int distinct;
    public String min;
    public String max;

    public Histogram histogram;

    public ColumnStatistics() {
    }

    public ColumnStatistics(String name, String dataType) {
        this.name = name;
        this.dataType = dataType;
    }
}

