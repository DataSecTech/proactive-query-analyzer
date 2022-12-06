package com.datasectech.queryanalyzer.core.query.modelbuilders;

public class InMemoryModelBuilder {

    protected String redisHost;
    protected int redisPort;
    protected boolean redisSSL;
    protected String keyPrefix;

    public InMemoryModelBuilder(String redisHost, int redisPort, boolean redisSSL, String keyPrefix) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisSSL = redisSSL;
        this.keyPrefix = keyPrefix;
    }

    public String build(String schemaName) {
        return InlineModelGenerator.generateInMemoryAnalyzerModel(schemaName, redisHost, redisPort, redisSSL, keyPrefix);
    }
}
