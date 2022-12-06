package com.datasectech.queryanalyzer.core.query;

import com.datasectech.queryanalyzer.core.query.modelbuilders.InlineModelGenerator;
import org.junit.Assert;
import org.junit.Test;

public class InlineModelGeneratorsTest {

    @Test
    public void csvInlineModel() {
        String csvInline = InlineModelGenerator.generateInlineCSVModel("DEFAULT", ".");

        String expected = "inline:{\n" +
                "  \"version\": \"1.0\",\n" +
                "  \"defaultSchema\": \"DEFAULT\",\n" +
                "  \"schemas\": [\n" +
                "    {\n" +
                "      \"type\": \"custom\",\n" +
                "      \"name\": \"DEFAULT\",\n" +
                "      \"factory\": \"org.apache.calcite.adapter.csv.CsvSchemaFactory\",\n" +
                "      \"operand\": {\n" +
                "        \"directory\": \".\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Assert.assertEquals(expected, csvInline);
    }

    @Test
    public void inMemoryModel() {
        String inMemoryAnalyzerModel = InlineModelGenerator.generateInMemoryAnalyzerModel("DEFAULT", "127.0.0.1", 6379, true, "AnalyzerDB");
        String expected = "inline:{\n" +
                "  \"version\": \"1.0\",\n" +
                "  \"defaultSchema\": \"DEFAULT\",\n" +
                "  \"schemas\": [\n" +
                "    {\n" +
                "      \"type\": \"custom\",\n" +
                "      \"name\": \"DEFAULT\",\n" +
                "      \"factory\": \"com.datasectech.queryanalyzer.core.db.cache.AnalyzerDbCacheSchemaFactory\",\n" +
                "      \"operand\": {\n" +
                "        \"redisHost\": \"127.0.0.1\",\n" +
                "        \"redisPort\": 6379,\n" +
                "        \"redisSSL\": true,\n" +
                "        \"keyPrefix\": \"AnalyzerDB\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Assert.assertEquals(expected, inMemoryAnalyzerModel);
    }
}
