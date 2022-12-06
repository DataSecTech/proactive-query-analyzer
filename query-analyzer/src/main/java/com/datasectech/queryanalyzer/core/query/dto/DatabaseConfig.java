package com.datasectech.queryanalyzer.core.query.dto;

import java.util.Map;

public class DatabaseConfig {
    public String calciteModel;
    public String schema;
    public String planDir;
    public Map<String, String> sensitiveColumns;
}
