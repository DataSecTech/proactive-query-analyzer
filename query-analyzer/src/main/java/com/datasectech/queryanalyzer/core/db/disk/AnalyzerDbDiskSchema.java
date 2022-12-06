package com.datasectech.queryanalyzer.core.db.disk;

import com.datasectech.queryanalyzer.core.db.AnalyzerDbSchema;
import com.datasectech.queryanalyzer.core.query.DataMapper;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Map;

public class AnalyzerDbDiskSchema extends AnalyzerDbSchema {

    private final static Logger logger = LogManager.getLogger(AnalyzerDbDiskSchema.class);

    private final File schemaStatsDirectory;

    public AnalyzerDbDiskSchema(String name, File schemaStatsDirectory) {
        super(name);

        logger.info("Creating AnalyzerDB schema {} from directory {}", name, schemaStatsDirectory);
        this.schemaStatsDirectory = schemaStatsDirectory;
    }

    @Override
    public Map<String, TableStatistics> readSchemaStatistics() {
        return new DataMapper().readSchemaStatistics(schemaStatsDirectory);
    }
}
