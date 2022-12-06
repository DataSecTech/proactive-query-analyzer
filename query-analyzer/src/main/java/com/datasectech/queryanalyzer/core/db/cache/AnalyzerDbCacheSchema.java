package com.datasectech.queryanalyzer.core.db.cache;

import com.datasectech.queryanalyzer.core.db.AnalyzerDbSchema;
import com.datasectech.queryanalyzer.core.db.cache.mapper.CacheMapperFacade;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class AnalyzerDbCacheSchema extends AnalyzerDbSchema {

    private final static Logger logger = LogManager.getLogger(AnalyzerDbCacheSchema.class);
    protected final CacheMapperFacade mapper;

    public AnalyzerDbCacheSchema(String name, CacheMapperFacade mapper) {
        super(name);

        logger.info("Creating AnalyzerDB schema {} from cache", name);
        this.mapper = mapper;
    }

    @Override
    public Map<String, TableStatistics> readSchemaStatistics() {
        return mapper.readSchemaStats(this.name);
    }
}
