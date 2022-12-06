package com.datasectech.queryanalyzer.core.query.sensitivity;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import com.datasectech.queryanalyzer.core.query.dto.TableColumnName;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.metadata.MetadataReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CalciteMetadataStore {

    public final Map<String, TableStatistics> schemaStats;
    public final Map<RelNode, List<TableColumnName>> projections;

    protected final MetadataReader metadataReader;
    protected final Map<String, String> additionalSensitiveColumns;

    public CalciteMetadataStore(MetadataReader metadataReader, Map<String, String> additionalSensitiveColumns) {
        this.metadataReader = metadataReader;
        this.additionalSensitiveColumns = additionalSensitiveColumns;

        this.schemaStats = metadataReader.schemaStats;
        this.projections = new HashMap<>();
    }

    public void buildTableStatistics() {
        metadataReader.calculateTableStatistics();
        metadataReader.addAdditionalSensitiveColumns(additionalSensitiveColumns);
    }

    public ColumnStatistics getColumnStatistics(RelNode rel, RexInputRef inputRef) {
        RelColumnOrigin origin = findColumnOrigin(rel, inputRef);
        List<String> qualifiedTableName = origin.getOriginTable().getQualifiedName();

        TableStatistics tableStatistics = schemaStats.get(qualifiedTableName.get(1));
        String columnName = origin.getOriginTable().getRowType().getFieldNames().get(origin.getOriginColumnOrdinal());

        return tableStatistics.columnStatisticsMap.get(columnName);
    }

    public TableStatistics getTableStatistics(RelNode rel, RexInputRef inputRef) {
        RelColumnOrigin origin = findColumnOrigin(rel, inputRef);
        List<String> qualifiedTableName = origin.getOriginTable().getQualifiedName();

        return schemaStats.get(qualifiedTableName.get(1));
    }

    public RelColumnOrigin findColumnOrigin(RelNode rel, RexInputRef inputRef) {

        if ("LogicalAggregate".equals(rel.getRelTypeName())) {
            throw new RuntimeException("Can not find column origin for LogicalAggregate node");
        }

        int columnIndex = inputRef.getIndex();
        RelColumnOrigin origin = RelMetadataQuery.instance().getColumnOrigin(rel, columnIndex);

        // Normal case - e.g simple filter
        if (origin != null) {
            return origin;
        }

        // Complex case - Outer joins (left, right, full)

        Set<RelColumnOrigin> origins = RelMetadataQuery.instance().getColumnOrigins(rel, columnIndex);

        for (RelColumnOrigin relColumnOrigin : origins) {

            // TODO handle multiple source, may be save that information in TableColumnName object
            if (relColumnOrigin != null) {
                RelDataTypeField f = relColumnOrigin.getOriginTable()
                        .getRowType()
                        .getFieldList()
                        .get(relColumnOrigin.getOriginColumnOrdinal());

                return relColumnOrigin;
            }
        }

        throw new RuntimeException("Column origin not found");
    }

    public boolean isSensitive(TableColumnName tableColumnName) {
        return getColumnSensitiveType(tableColumnName) != null;
    }

    public String getColumnSensitiveType(TableColumnName tableColumnName) {

        if (!schemaStats.containsKey(tableColumnName.table)) {
            return null;
        }

        return schemaStats.get(tableColumnName.table).sensitiveColumns
                .get(tableColumnName.getTableColumnName());
    }

    public TableColumnName findTableAndColumn(RelNode rel, RexInputRef inputRef, String name) {

        if ("LogicalAggregate".equals(rel.getRelTypeName())) {
            return new TableColumnName(aggregateNodeName(rel), name);
        }

        RelColumnOrigin origin = findColumnOrigin(rel, inputRef);

        String tableName = origin
                .getOriginTable()
                .getQualifiedName()
                .get(1);

        String columnName = origin.getOriginTable()
                .getRowType()
                .getFieldList()
                .get(origin.getOriginColumnOrdinal())
                .getName();

        return new TableColumnName(tableName, columnName);
    }

    public static String aggregateNodeName(RelNode rel) {
        return "LogicalAggregate-" + rel.getId();
    }
}
