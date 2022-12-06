package com.datasectech.queryanalyzer.core.query;

public class QueryBuilder {

    public static String createSQL(String colName) {

        return String.format(" count(distinct %s) as %s_distinct_count, " +
                        "count(%s) as %s_notnull_count, " +
                        "min(%s) as %s_min_value, " +
                        "max(%s) as %s_max_value,",
                colName, colName, colName, colName, colName, colName, colName, colName);
    }
}
