package com.datasectech.queryanalyzer.core.query.dto;

public class TableColumnName {

    public String table;
    public String column;

    public TableColumnName(String table, String column) {
        this.table = table;
        this.column = column;
    }

    public String getTableColumnName() {
        return String.format("%s.%s", table, column);
    }
}
