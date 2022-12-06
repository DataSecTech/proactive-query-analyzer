package com.datasectech.queryanalyzer.core.query.sensitivity.metadata;

import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;

import java.util.Map;

public abstract class MetadataReader {

    public final Map<String, TableStatistics> schemaStats;

    public MetadataReader(Map<String, TableStatistics> schemaStats) {
        this.schemaStats = schemaStats;
    }

    public abstract void calculateTableStatistics();

    public void addAdditionalSensitiveColumns(Map<String, String> sensitiveColumns) {

        if (sensitiveColumns == null) {
            return;
        }

        for (String tableColumn : sensitiveColumns.keySet()) {

            String[] parts = tableColumn.toUpperCase().split("\\.");
            if (parts.length != 2) {
                throw new RuntimeException("Sensitive column names must be 'table.column' format");
            }

            String table = parts[0];

            if (!schemaStats.containsKey(table)) {
                throw new RuntimeException("Sensitive table " + table + " not found in schema");
            }

            schemaStats.get(table).sensitiveColumns.put(tableColumn.toUpperCase(), sensitiveColumns.get(tableColumn));
        }
    }
}
