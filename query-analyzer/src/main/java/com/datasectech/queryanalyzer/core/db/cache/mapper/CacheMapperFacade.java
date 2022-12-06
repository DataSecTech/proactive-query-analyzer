package com.datasectech.queryanalyzer.core.db.cache.mapper;

import com.datasectech.queryanalyzer.core.db.cache.CacheDataFormat;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class CacheMapperFacade {

    protected final CacheDataFormat dataFormat;
    protected final CacheMapper cacheMapper;

    public CacheMapperFacade(String redisHost, int redisPort, boolean ssl, String keyPrefix, CacheDataFormat dataFormat) {
        this(new Jedis(redisHost, redisPort, ssl), keyPrefix, dataFormat);
    }

    public CacheMapperFacade(Jedis redisClient, String keyPrefix, CacheDataFormat dataFormat) {
        this.dataFormat = dataFormat;
        switch (dataFormat) {
            case INDIVIDUAL:
                cacheMapper = new IndividualCacheMapper(redisClient, keyPrefix);
                break;
            default:
                throw new RuntimeException("Unknown data format: " + dataFormat);
        }
    }

    public void writeSchemaStats(String schemaName, Map<String, TableStatistics> schemaStats) {
        cacheMapper.writeSchemaStats(schemaName, schemaStats);
    }

    public Map<String, TableStatistics> readSchemaStats(String schemaName) {
        return cacheMapper.readSchemaStats(schemaName);
    }
}
