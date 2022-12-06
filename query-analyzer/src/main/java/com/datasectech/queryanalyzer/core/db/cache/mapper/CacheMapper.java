package com.datasectech.queryanalyzer.core.db.cache.mapper;

import com.datasectech.queryanalyzer.core.query.DataMapper;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;

import java.util.Map;

public abstract class CacheMapper {

    protected final Jedis redisClient;
    protected final String keyPrefix;
    protected final ObjectMapper jsonMapper;

    public CacheMapper(Jedis redisClient, String keyPrefix) {
        this.redisClient = redisClient;
        this.keyPrefix = keyPrefix;
        this.jsonMapper = DataMapper.buildJsonMapper();
    }

    public String schemaKey(String schemaName) {
        return String.format("%s:%s", this.keyPrefix, schemaName);
    }

    public String tableKey(String schemaName, String tableName) {
        return String.format("%s:%s.%s", this.keyPrefix, schemaName, tableName);
    }

    public abstract Map<String, TableStatistics> readSchemaStats(String schemaName);

    public abstract void writeSchemaStats(String schemaName, Map<String, TableStatistics> schemaStats);
}
