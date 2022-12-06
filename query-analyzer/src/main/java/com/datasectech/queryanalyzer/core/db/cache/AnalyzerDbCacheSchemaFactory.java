package com.datasectech.queryanalyzer.core.db.cache;

import com.datasectech.queryanalyzer.core.db.cache.mapper.CacheMapperFacade;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class AnalyzerDbCacheSchemaFactory implements SchemaFactory {

    public static final String DEFAULT_PREFIX = "AnalyzerDB";
    private final static Logger logger = LogManager.getLogger(AnalyzerDbCacheSchemaFactory.class);

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        logger.debug("Creating schema: {}", name);

        String redisHost = String.valueOf(operand.getOrDefault("redisHost", "127.0.0.1"));
        String redisPortStr = String.valueOf(operand.getOrDefault("redisPort", "6379"));
        String redisSSLStr = String.valueOf(operand.getOrDefault("redisSSL", "false"));

        String keyPrefix = String.valueOf(operand.getOrDefault("keyPrefix", DEFAULT_PREFIX));

        int redisPort = Integer.parseInt(redisPortStr);
        boolean redisSSL = Boolean.parseBoolean(redisSSLStr);

        CacheMapperFacade mapper = new CacheMapperFacade(redisHost, redisPort, redisSSL, keyPrefix, CacheDataFormat.INDIVIDUAL);

        return new AnalyzerDbCacheSchema(name, mapper);
    }
}
