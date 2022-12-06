package com.datasectech.queryanalyzer.core.query.dto;

import java.util.HashMap;
import java.util.Map;

public class Histogram {

    public String histogramType;
    public String columnName;

    public Map<String, Bucket> bucketMap = new HashMap<>();

    public Histogram() {
    }

    public Histogram(String histogramType, String columnName) {
        this.histogramType = histogramType;
        this.columnName = columnName;
    }
}
