package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.DataMapper;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class CalciteTestUtils {

    public static final double DELTA = 0.0001;
    protected static ObjectMapper objectMapper;

    public static RelBuilder getRelBuilder() throws SQLException {
        File modelFile = new File("data/sales/model-sales.yml");

        Properties info = new Properties();
        info.setProperty("model", modelFile.toString());

        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus schema = calciteConnection.getRootSchema()
                .getSubSchema("SALES");

        FrameworkConfig config = Frameworks
                .newConfigBuilder()
                .defaultSchema(schema)
                .parserConfig(SqlParser.configBuilder().setCaseSensitive(false).build())
                .build();

        return RelBuilder.create(config);
    }

    protected static ObjectMapper objectMapper() {
        if (objectMapper == null) {
            objectMapper = DataMapper.buildJsonMapper();
        }

        return objectMapper;
    }

    public static TableStatistics readTableStatistics(String resourcePath) {
        ClassLoader classLoader = CalciteTestUtils.class.getClassLoader();

        try (InputStream inputStream = classLoader.getResourceAsStream(resourcePath)) {

            if (inputStream == null) {
                throw new RuntimeException("Can not build input stream from resource path: " + resourcePath);
            }

            String statsString = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            return objectMapper().readValue(statsString, TableStatistics.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, TableStatistics> readSalesStatistic() {
        final ImmutableMap.Builder<String, TableStatistics> builder = ImmutableMap.builder();

        builder.put("DEPTS", readTableStatistics("sales/stats/depts.json"));
        builder.put("EMPS", readTableStatistics("sales/stats/emps.json"));
        builder.put("PERSONS", readTableStatistics("sales/stats/persons.json"));
        builder.put("SDEPTS", readTableStatistics("sales/stats/sdepts.json"));

        return builder.build();
    }
}
