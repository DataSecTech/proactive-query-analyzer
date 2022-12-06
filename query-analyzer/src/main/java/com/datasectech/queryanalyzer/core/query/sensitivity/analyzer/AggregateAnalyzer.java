package com.datasectech.queryanalyzer.core.query.sensitivity.analyzer;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import com.datasectech.queryanalyzer.core.query.dto.TableColumnName;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.CalciteMetadataStore;
import com.datasectech.queryanalyzer.core.query.sensitivity.TraversalContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datasectech.queryanalyzer.core.query.SDLRelTraverser.extractValue;

public class AggregateAnalyzer extends RelNodeAnalyzer {

    public AggregateAnalyzer(TraversalContext traversalContext) {
        super(traversalContext);
    }

    public void analyze(RelNode rel, List<Pair<String, Object>> values, List<RelNode> inputs) {

        Object group = extractValue("group", values);
        ImmutableBitSet bit = (ImmutableBitSet) group;

        RelNode project = inputs.get(0);

        List<TableColumnName> refs = metadataStore.projections.get(project);
        if (refs == null) {
            throw new RuntimeException("Can not find project node of aggregation");
        }

        List<Integer> groupByIndexes = bit.asList();

        if (groupByIndexes.size() == 0) {
            // Only have the expressions as output
            nodeStats.put(rel, new SensitivityStatistics(1, 1, Collections.emptyMap()));
            generateColumnStats(rel, values, inputs, groupByIndexes, 1);

            return;
        }

        if (groupByIndexes.size() == 1) {

            TableColumnName tableColumnName = refs.get(groupByIndexes.get(0));

            TableStatistics schemaStats = metadataStore.schemaStats.get(tableColumnName.table);
            ColumnStatistics columnStats = schemaStats.columnStatisticsMap.get(tableColumnName.column);

            String sensitiveType = metadataStore.getColumnSensitiveType(tableColumnName);
            Map<String, String> sensitiveColumns = new HashMap<>();
            if (sensitiveType != null) {
                sensitiveColumns.put(tableColumnName.getTableColumnName(), sensitiveType);
            }

            nodeStats.put(rel, new SensitivityStatistics(1, columnStats.distinct, sensitiveColumns));
            generateColumnStats(rel, values, inputs, groupByIndexes, columnStats.distinct);

            return;
        }

        // For accurate prediction we need count of distinct (col_1, ..., col_n),
        // worst case same as the table rows

        Map<String, String> sensitiveColumns = new HashMap<>();
        String tableName = null;

        for (Integer columnBit : groupByIndexes) {
            TableColumnName tableColumnName = refs.get(columnBit);

            if (tableColumnName == null) {
                throw new RuntimeException("Can not find aggregation column in input node");
            }

            metadataStore.getColumnSensitiveType(tableColumnName);

            String sensitiveType = metadataStore.getColumnSensitiveType(tableColumnName);
            if (sensitiveType != null) {
                sensitiveColumns.put(tableColumnName.getTableColumnName(), sensitiveType);
            }

            if (tableName == null) {
                tableName = tableColumnName.table;
            } else if (!tableName.equals(tableColumnName.table)) {
                throw new RuntimeException("Aggregation on multiple tables is not implemented yet");
            }
        }

        TableStatistics inputTableStats = metadataStore.schemaStats.get(tableName);

        nodeStats.put(rel, new SensitivityStatistics(1, inputTableStats.totalRows, sensitiveColumns));
        generateColumnStats(rel, values, inputs, groupByIndexes, inputTableStats.totalRows);
    }

    public void generateColumnStats(
            RelNode rel, List<Pair<String, Object>> values,
            List<RelNode> inputs, List<Integer> groupByIndexes, int rows
    ) {
        RelNode project = inputs.get(0);
        List<TableColumnName> refs = metadataStore.projections.get(project);

        String relationName = CalciteMetadataStore.aggregateNodeName(rel);

        TableStatistics aggregateStats = new TableStatistics();

        aggregateStats.name = relationName;
        aggregateStats.totalRows = rows;

        metadataStore.schemaStats.put(relationName, aggregateStats);

        // Aggregate function calls

        for (Pair<String, Object> value : values) {

            if (value.right instanceof AggregateCall) {

                ColumnStatistics colStats = new ColumnStatistics();
                // Worst case - estimated rows

                colStats.distinct = rows;
                colStats.isPii = false;
                colStats.dataType = "INTEGER";

                AggregateCall aggregateCall = (AggregateCall) value.right;
                colStats.name = aggregateCall.getName();

                // Special case for min and max

                if (aggregateCall.getArgList().size() == 1) {

                    int index = aggregateCall.getArgList().get(0);
                    TableColumnName tableColumnName = refs.get(index);

                    if (SqlKind.MIN.equals(aggregateCall.getAggregation().kind)) {

                        colStats.min = metadataStore.schemaStats.get(tableColumnName.table)
                                .columnStatisticsMap.get(tableColumnName.column).min;

                    } else if (SqlKind.MAX.equals(aggregateCall.getAggregation().kind)) {

                        colStats.max = metadataStore.schemaStats.get(tableColumnName.table)
                                .columnStatisticsMap.get(tableColumnName.column).max;
                    }
                }

                aggregateStats.columnStatisticsMap.put(aggregateCall.name, colStats);
            }
        }

        // GroupBy columns
        for (Integer groupByIndex : groupByIndexes) {
            TableColumnName tableColumnName = refs.get(groupByIndex);

            if (tableColumnName == null) {
                throw new RuntimeException("Can not find aggregation column in input node");
            }

            String sensitiveType = metadataStore.getColumnSensitiveType(tableColumnName);

            if (sensitiveType != null) {
                aggregateStats.sensitiveColumns.put(tableColumnName.getTableColumnName(), sensitiveType);
            }

            ColumnStatistics col = metadataStore.schemaStats.get(tableColumnName.table)
                    .columnStatisticsMap.get(tableColumnName.column);

            aggregateStats.columnStatisticsMap.put(tableColumnName.column, col);
        }
    }
}
