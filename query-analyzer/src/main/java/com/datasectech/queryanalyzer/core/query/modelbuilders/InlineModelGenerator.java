package com.datasectech.queryanalyzer.core.query.modelbuilders;

import com.datasectech.queryanalyzer.core.db.cache.AnalyzerDbCacheSchemaFactory;
import org.apache.calcite.util.JsonBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InlineModelGenerator {

    public static String generateInMemoryAnalyzerModel(String schemaName, String redisHost, int redisPort, boolean redisSSL, String keyPrefix) {
        Map<String, Object> operandMap = new HashMap<>();
        operandMap.put("redisHost", redisHost);
        operandMap.put("redisPort", redisPort);
        operandMap.put("redisSSL", redisSSL);
        operandMap.put("keyPrefix", keyPrefix);

        return generateInlineModel(schemaName, AnalyzerDbCacheSchemaFactory.class.getCanonicalName(), operandMap);
    }

    public static String generateInlineCSVModel(String schemaName, String directory) {
        Map<String, Object> operandMap = new HashMap<>();
        operandMap.put("directory", directory);

        return generateInlineModel(schemaName, "org.apache.calcite.adapter.csv.CsvSchemaFactory", operandMap);
    }

    protected static String generateInlineModel(String schemaName, String factory, Map<String, Object> operandMap) {
        final JsonBuilder json = new JsonBuilder();

        final Map<String, Object> root = json.map();
        root.put("version", "1.0");
        root.put("defaultSchema", schemaName);

        final List<Object> schemaList = json.list();
        root.put("schemas", schemaList);

        final Map<String, Object> schema = json.map();
        schemaList.add(schema);

        schema.put("type", "custom");
        schema.put("name", schemaName);
        schema.put("factory", factory);

        schema.put("operand", operandMap);

        return "inline:" + json.toJsonString(root);
    }
}
