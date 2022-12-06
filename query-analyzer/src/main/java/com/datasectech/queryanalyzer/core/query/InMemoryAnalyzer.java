package com.datasectech.queryanalyzer.core.query;

import com.datasectech.queryanalyzer.core.db.cache.AnalyzerDbCacheSchemaFactory;
import com.datasectech.queryanalyzer.core.db.cache.CacheDataFormat;
import com.datasectech.queryanalyzer.core.db.cache.mapper.CacheMapperFacade;
import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.datasectech.queryanalyzer.core.query.modelbuilders.InMemoryModelBuilder;
import com.datasectech.queryanalyzer.core.query.sensitivity.SensitivityAnalyzer;
import com.datasectech.queryanalyzer.core.query.sensitivity.SensitivityAnalyzerBuilder;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class InMemoryAnalyzer {

    private final static Logger logger = LogManager.getLogger(InMemoryAnalyzer.class);

    protected CacheMapperFacade mapper;
    protected InMemoryModelBuilder modelBuilder;
    protected Map<String, FrameworkConfig> calciteFrameworkConfigs;

    public InMemoryAnalyzer(String redisHost, int redisPort, boolean redisSSL, String keyPrefix) {
        this.modelBuilder = new InMemoryModelBuilder(redisHost, redisPort, redisSSL, keyPrefix);
        this.mapper = new CacheMapperFacade(redisHost, redisPort, redisSSL, keyPrefix, CacheDataFormat.INDIVIDUAL);
        this.calciteFrameworkConfigs = new HashMap<>();
    }

    public InMemoryAnalyzer(String redisHost, int redisPort, boolean redisSSL) {
        this(redisHost, redisPort, redisSSL, AnalyzerDbCacheSchemaFactory.DEFAULT_PREFIX);
    }

    public InMemoryAnalyzer(Jedis redisClient, String keyPrefix, InMemoryModelBuilder modelBuilder) {
        this.mapper = new CacheMapperFacade(redisClient, keyPrefix, CacheDataFormat.INDIVIDUAL);
        this.modelBuilder = modelBuilder;
        this.calciteFrameworkConfigs = new HashMap<>();
    }

    protected FrameworkConfig buildFrameworkConfig(String schemaName) {
        Properties info = new Properties();
        info.setProperty("model", modelBuilder.build(schemaName));

        CalciteConnection calciteConnection;
        try {
            Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
            calciteConnection = connection.unwrap(CalciteConnection.class);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        SchemaPlus schema = calciteConnection.getRootSchema()
                .getSubSchema(schemaName);

        if (schema == null) {
            throw new RuntimeException("Can not find schema named: " + schemaName);
        }

        return Frameworks
                .newConfigBuilder()
                .defaultSchema(schema)
                .parserConfig(SqlParser.configBuilder().setCaseSensitive(false).build())
                .build();
    }

    protected FrameworkConfig getFrameworkConfig(String schemaName) {
        if (!calciteFrameworkConfigs.containsKey(schemaName)) {
            calciteFrameworkConfigs.put(schemaName, buildFrameworkConfig(schemaName));
        }

        return calciteFrameworkConfigs.get(schemaName);
    }

    public SensitivityStatistics analyze(String schemaName, String query, Map<String, String> sensitiveColumns) {

        FrameworkConfig config = getFrameworkConfig(schemaName);

        try (Planner planner = Frameworks.getPlanner(config)) {
            SqlNode parsed = planner.parse(query);

            SqlNode validate = planner.validate(parsed);
            RelRoot relRoot = planner.rel(validate);

            Map<String, TableStatistics> tableStats = mapper.readSchemaStats(schemaName);


            SensitivityAnalyzer sensitivityAnalyzer = new SensitivityAnalyzerBuilder()
                    // Expecting the table statistics already contain sensitive column hash map
                    .fixedMetadataReader(tableStats, sensitiveColumns == null ? Collections.emptyMap() : sensitiveColumns)
                    .build();

            return sensitivityAnalyzer.analyze(relRoot.rel);

        } catch (SqlParseException | ValidationException | RelConversionException e) {
            throw new SDLQueryAnalyzeException(e);
        }
    }
}
