package com.datasectech.queryanalyzer.core.query;

import com.datasectech.queryanalyzer.core.query.dto.CalciteModel;
import com.datasectech.queryanalyzer.core.query.dto.Input;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DataMapper {

    private final static Logger logger = LogManager.getLogger(DataMapper.class);
    protected ObjectMapper yamlMapper;
    protected ObjectMapper jsonMapper;

    protected ObjectMapper getYamlMapper() {

        if (yamlMapper == null) {
            yamlMapper = new ObjectMapper(new YAMLFactory());
            yamlMapper.setVisibility(
                    VisibilityChecker.Std.defaultInstance()
                            .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
            );
        }

        return yamlMapper;
    }

    public static ObjectMapper buildJsonMapper() {
        return new ObjectMapper()
                .setVisibility(
                        VisibilityChecker.Std.defaultInstance()
                                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                );
    }

    public ObjectMapper getJsonMapper() {

        if (jsonMapper == null) {
            jsonMapper = buildJsonMapper();
        }

        return jsonMapper;
    }

    public <T> T readDataYaml(File file, String fileType, Class<T> valueType) {
        if (!file.exists()) {
            throw new RuntimeException("File for " + fileType + " not found at " + file);
        }

        try {
            return getYamlMapper().readValue(file, valueType);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + fileType + "  file", e);
        }
    }

    private <T> T readDataJson(File file, String fileType, Class<T> valueType) {
        if (!file.exists()) {
            throw new RuntimeException("File for " + fileType + " not found at " + file);
        }

        try {
            return getJsonMapper().readValue(file, valueType);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + fileType + "  file", e);
        }
    }

    public Input readInput(File file) {
        return readDataYaml(file, "queries", Input.class);
    }

    public CalciteModel readModel(File file) {
        return readDataYaml(file, "calcite model", CalciteModel.class);
    }

    public CalciteModel readModel(String content) {
        try {
            return getJsonMapper().readValue(content, CalciteModel.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read calcite model string", e);
        }
    }

    public TableStatistics readTableStatisticsYaml(File file) {
        return readDataYaml(file, "table statistics", TableStatistics.class);
    }

    public TableStatistics readTableStatisticsJson(File file) {
        return readDataJson(file, "table statistics", TableStatistics.class);
    }

    public void writeDataJson(File file, String fileType, boolean overwrite, Object value) {
        if (file.exists() && !overwrite) {
            throw new RuntimeException("File " + file + " exists; not overwriting.");
        }

        try {
            logger.info("Writing {} file: {}", fileType, file);
            getJsonMapper().writeValue(file, value);

        } catch (IOException e) {
            throw new RuntimeException("Failed to write file: " + file, e);
        }
    }

    public String writeDataJson(Object value) {
        try {
            return getJsonMapper().writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException("Failed create json string", e);
        }
    }

    public void writeTableStatisticsJson(File file, boolean overwrite, TableStatistics tableStatistics) {
        writeDataJson(file, "table statistics", overwrite, tableStatistics);
    }

    public Map<String, TableStatistics> readSchemaStatistics(File directory) {

        Map<String, TableStatistics> schemaStats = new HashMap<>();
        File[] files = directory.listFiles();

        if (files == null) {
            logger.warn("No files found in {}", directory);
            return schemaStats;
        }

        for (File file : Objects.requireNonNull(files)) {

            if (file.toString().endsWith(".yml")) {

                String tableName = file.getName().replace(".yml", "").toUpperCase();
                schemaStats.put(tableName, readTableStatisticsYaml(file));

            } else if (file.toString().endsWith(".json")) {

                String tableName = file.getName().replace(".json", "").toUpperCase();
                schemaStats.put(tableName, readTableStatisticsJson(file));
            }
        }

        if (schemaStats.isEmpty()) {
            logger.warn("No schema statistics files found in {}", directory);
        }

        return schemaStats;
    }

    public void createDirectoryIfNotExists(File dir) {
        if (!dir.exists()) {
            logger.info("Creating directory: {}", dir);

            boolean result = dir.mkdirs();
            if (!result) {
                throw new RuntimeException("Failed to create directory: " + dir);
            }
        }
    }

    public void writeSchemaStatistics(File directory, Map<String, TableStatistics> schemaStats, boolean overwrite) {
        createDirectoryIfNotExists(directory);

        for (String table : schemaStats.keySet()) {
            try {

                File tableStatFile = new File(directory, table.toLowerCase() + ".json");
                writeTableStatisticsJson(tableStatFile, overwrite, schemaStats.get(table));

            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }
}
