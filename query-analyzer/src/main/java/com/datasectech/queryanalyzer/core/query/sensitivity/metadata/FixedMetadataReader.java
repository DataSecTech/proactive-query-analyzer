package com.datasectech.queryanalyzer.core.query.sensitivity.metadata;

import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;

import java.util.Map;

public class FixedMetadataReader extends MetadataReader {

    public FixedMetadataReader(Map<String, TableStatistics> schemaStats) {
        super(schemaStats);
    }

    @Override
    public void calculateTableStatistics() {
        // Do nothing
    }
}
