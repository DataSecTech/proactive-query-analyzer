package com.datasectech.queryanalyzer.core.db.cache.mapper;

import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IndividualCacheMapper extends CacheMapper {

    public IndividualCacheMapper(Jedis redisClient, String keyPrefix) {
        super(redisClient, keyPrefix);
    }

    public void writeSchemaStats(String schemaName, Map<String, TableStatistics> schemaStats) {
        try {
            String tablesJson = jsonMapper.writeValueAsString(schemaStats.keySet());
            redisClient.set(schemaKey(schemaName), tablesJson);

            for (String table : schemaStats.keySet()) {
                String tableKey = tableKey(schemaName, table);
                redisClient.set(tableKey, jsonMapper.writeValueAsString(schemaStats.get(table)));
            }

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, TableStatistics> readSchemaStats(String schemaName) {

        Map<String, TableStatistics> stats = new HashMap<>();
        String schemaKey = schemaKey(schemaName);
        String tablesJson = redisClient.get(schemaKey);

        if (tablesJson == null) {
            throw new RuntimeException("Table list not found in cache with key " + schemaName);
        }

        try {
            Set<String> tables = jsonMapper.readValue(tablesJson, new TypeReference<Set<String>>() {
            });

            for (String table : tables) {

                String tableKey = tableKey(schemaName, table);
                String statistics = redisClient.get(tableKey);

                if (statistics == null) {
                    throw new RuntimeException("Can not find table statistics with key " + tableKey);
                }

                TableStatistics tableStatistics = jsonMapper.readValue(statistics, TableStatistics.class);
                stats.put(table, tableStatistics);
            }

            return stats;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
