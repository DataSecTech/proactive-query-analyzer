package com.datasectech.queryanalyzer.core.query.dto;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

// If table statistics structure is changed update `analyzer-stats` in `sdl-cluster/node/data/hive` with
//
//    sdl-dev dependency -a analyzer-db-stats
//
public class TableStatistics {
    public int totalRows;
    public String name;
    public Map<String, String> sensitiveColumns = new HashMap<>();

    public Map<String, ColumnStatistics> columnStatisticsMap = new LinkedHashMap<>();
}
