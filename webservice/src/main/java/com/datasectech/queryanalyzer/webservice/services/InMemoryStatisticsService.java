package com.datasectech.queryanalyzer.webservice.services;

import com.datasectech.queryanalyzer.core.db.cache.mapper.IndividualCacheMapper;
import com.datasectech.queryanalyzer.core.query.InMemoryAnalyzer;
import com.datasectech.queryanalyzer.core.query.SDLQueryAnalyzeException;
import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.datasectech.queryanalyzer.core.query.modelbuilders.InMemoryModelBuilder;
import com.datasectech.queryanalyzer.webservice.dto.QueryList;
import com.datasectech.queryanalyzer.webservice.dto.ResultList;
import com.datasectech.queryanalyzer.webservice.entities.SchemaEntity;
import com.datasectech.queryanalyzer.webservice.entities.TableEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class InMemoryStatisticsService {

    public static final Logger logger = LoggerFactory.getLogger(InMemoryStatisticsService.class);
    protected ObjectMapper objectMapper;
    protected SchemaService schemaService;
    protected String redisHost;
    protected int redisPort;
    protected int redisDbIndex;
    protected boolean redisSSL;
    protected String keyPrefix;
    protected InMemoryModelBuilder modelBuilder;

    public InMemoryStatisticsService(
            ObjectMapper objectMapper,
            SchemaService schemaService,
            @Value("${secure-dl.redis.host}") String redisHost,
            @Value("${secure-dl.redis.port}") int redisPort,
            @Value("${secure-dl.redis.db-index}") int redisDbIndex,
            @Value("${secure-dl.redis.ssl}") boolean redisSSL,
            @Value("${secure-dl.redis.key-prefix}") String keyPrefix
    ) {
        this.objectMapper = objectMapper;
        this.schemaService = schemaService;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisDbIndex = redisDbIndex;
        this.redisSSL = redisSSL;
        this.keyPrefix = keyPrefix;

        this.modelBuilder = new InMemoryModelBuilder(redisHost, redisPort, redisSSL, keyPrefix);
    }

    public void checkOrUpdate(SchemaEntity schema, Jedis jedis) {
        IndividualCacheMapper cacheMapper = new IndividualCacheMapper(jedis, keyPrefix);

        Map<String, TableStatistics> schemaStats = new HashMap<>();

        List<String> keys = new ArrayList<>();
        keys.add(cacheMapper.schemaKey(schema.getName()));

        for (TableEntity table : schema.getTables()) {
            logger.info(table.getName());

            try {
                TableStatistics statistics = objectMapper.readValue(table.getStatistics(), TableStatistics.class);
                schemaStats.put(table.getName(), statistics);
                keys.add(cacheMapper.tableKey(schema.getName(), table.getName()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        long exists = jedis.exists(keys.toArray(new String[0]));
        if (exists != keys.size()) {
            logger.info("Updating schema keys");
            cacheMapper.writeSchemaStats(schema.getName(), schemaStats);
        }
    }

    public void checkOrUpdate(SchemaEntity schema) {
        try (Jedis jedis = new Jedis(redisHost, redisPort, redisSSL)) {
            jedis.select(redisDbIndex);
            checkOrUpdate(schema, jedis);
        }
    }

    public ResultList analyzeQueries(String schemaName, QueryList queryList) {
        SchemaEntity schema = schemaService.findByNameOrFail(schemaName);

        // Might consider moving to spring / bean context friendly redis connection object
        try (Jedis jedis = new Jedis(redisHost, redisPort, redisSSL)) {
            jedis.select(redisDbIndex);

            checkOrUpdate(schema, jedis);
            ResultList result = new ResultList();
            InMemoryAnalyzer inMemoryAnalyzer = new InMemoryAnalyzer(jedis, keyPrefix, modelBuilder);

            for (String queryId : queryList.queries.keySet()) {
                try {
                    String query = queryList.queries.get(queryId);
                    logger.info("Analyzing query [{}]: {}", queryId, query);

                    SensitivityStatistics statistics = inMemoryAnalyzer.analyze(schemaName, query, queryList.sensitiveColumns);
                    logger.info("Analysis result [{}]: {}", queryId, statistics);

                    result.success.put(queryId, statistics);

                } catch (SDLQueryAnalyzeException e) {
                    logger.info("Analysis failed [{}]: {}", queryId, e.getMessage());
                    result.errors.put(queryId, e.getMessage());
                }
            }

            result.summary.total = queryList.queries.size();
            result.summary.successes = result.success.size();
            result.summary.errors = result.errors.size();

            return result;
        }
    }
}
