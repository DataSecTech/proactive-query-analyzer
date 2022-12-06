package com.datasectech.queryanalyzer.core.db.cache.mapper;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes.CalciteTestUtils;
import com.fiftyonred.mock_jedis.MockJedis;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class IndividualCacheMapperTest {

    @Test
    public void writeAndReadSchemaStats() {
        // Newer version of the Jedis will try to connect to the socket
        Jedis redisClient = new MockJedis("test-redis");
        IndividualCacheMapper cacheMapper = new IndividualCacheMapper(redisClient, "test-key-prefix");

        Map<String, TableStatistics> expectedSchema = CalciteTestUtils.readSalesStatistic();

        String schemaName = "SALES";

        cacheMapper.writeSchemaStats(schemaName, expectedSchema);
        Map<String, TableStatistics> actualSchema = cacheMapper.readSchemaStats(schemaName);

        Assert.assertArrayEquals(
                expectedSchema.keySet().stream().sorted().toArray(),
                actualSchema.keySet().stream().sorted().toArray()
        );

        for (String table : expectedSchema.keySet()) {
            TableStatistics expectedTableStats = expectedSchema.get(table);
            TableStatistics actualTableStats = actualSchema.get(table);

            Assert.assertEquals(expectedTableStats.name, actualTableStats.name);
            Assert.assertEquals(expectedTableStats.totalRows, actualTableStats.totalRows);

            Assert.assertArrayEquals(
                    expectedTableStats.columnStatisticsMap.keySet().stream().sorted().toArray(),
                    actualTableStats.columnStatisticsMap.keySet().stream().sorted().toArray()
            );

            for (String actualColumn : actualTableStats.columnStatisticsMap.keySet()) {

                ColumnStatistics expectedColumnStatistics = expectedTableStats.columnStatisticsMap.get(actualColumn);
                ColumnStatistics actualColumnStatistics = actualTableStats.columnStatisticsMap.get(actualColumn);

                Assert.assertEquals(expectedColumnStatistics.name, actualColumnStatistics.name);
                Assert.assertEquals(expectedColumnStatistics.min, actualColumnStatistics.min);
                Assert.assertEquals(expectedColumnStatistics.max, actualColumnStatistics.max);
                Assert.assertEquals(expectedColumnStatistics.dataType, actualColumnStatistics.dataType);
                Assert.assertEquals(expectedColumnStatistics.distinct, actualColumnStatistics.distinct);
                Assert.assertEquals(expectedColumnStatistics.notNull, actualColumnStatistics.notNull);

                // Note: Not comparing histogram, yet.
            }
        }
    }
}