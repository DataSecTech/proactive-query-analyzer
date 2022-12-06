package com.datasectech.queryanalyzer.core.db;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AnalyzerDbSchema extends AbstractSchema {

    private final static Logger logger = LogManager.getLogger(AnalyzerDbSchema.class);

    private static final Map<String, SqlTypeName> typeNameMapping;

    static {
        typeNameMapping = new HashMap<>();
        typeNameMapping.put("INT", SqlTypeName.INTEGER);
        typeNameMapping.put("STRING", SqlTypeName.VARCHAR);
    }

    protected final String name;
    protected Map<String, Table> tableMap;

    public AnalyzerDbSchema(String name) {
        this.name = name;
    }

    @Override
    protected Map<String, Table> getTableMap() {

        if (tableMap == null) {
            tableMap = generateTableMap();
        }

        return tableMap;
    }

    public abstract Map<String, TableStatistics> readSchemaStatistics();

    protected SqlTypeName findSqlTypeName(String dataType) {

        if (dataType == null) {
            throw new RuntimeException("Can't find SqlDataType for null DataType");
        }

        String dataTypeUpper = dataType.toUpperCase();
        if (typeNameMapping.containsKey(dataTypeUpper)) {
            return typeNameMapping.get(dataTypeUpper);
        }

        SqlTypeName sqlTypeName = SqlTypeName.get(dataTypeUpper);
        if (sqlTypeName != null) {
            return sqlTypeName;
        }

        throw new RuntimeException(
                "Couldn't find SqlTypeName for input DataType: " + dataType
                        + "; update the predefined hash map as needed."
        );
    }

    protected Map<String, Table> generateTableMap() {
        logger.info("Generating TableMap of schema: {}", this.name);

        Map<String, TableStatistics> schemaStats = readSchemaStatistics();

        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        for (String table : schemaStats.keySet()) {

            List<String> columns = new ArrayList<>();
            List<SqlTypeName> types = new ArrayList<>();

            Map<String, ColumnStatistics> columnStats = schemaStats.get(table).columnStatisticsMap;

            for (String column : columnStats.keySet()) {
                columns.add(columnStats.get(column).name);

                SqlTypeName sqlType = findSqlTypeName(columnStats.get(column).dataType);
                types.add(sqlType);
            }

            logger.debug("Adding table {} to schema {}", table, this.name);
            builder.put(table, new AnalyzerTable(table, columns, types));
        }

        return builder.build();
    }
}
