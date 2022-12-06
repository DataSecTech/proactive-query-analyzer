package com.datasectech.queryanalyzer.core.query.dto;

import java.util.Map;

public class Input {
    public DatabaseConfig db;
    public Map<String, String> queries;

    public String getQuery(String queryId) {
        if (queries == null) {
            throw new RuntimeException("Queries map is empty.");
        }

        if (!queries.containsKey(queryId)) {
            throw new RuntimeException("Query with id '" + queryId + "' not found.");
        }

        return queries.get(queryId);
    }
}
