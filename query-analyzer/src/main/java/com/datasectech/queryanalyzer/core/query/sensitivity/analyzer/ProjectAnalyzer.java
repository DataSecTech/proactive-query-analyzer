package com.datasectech.queryanalyzer.core.query.sensitivity.analyzer;

import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import com.datasectech.queryanalyzer.core.query.dto.TableColumnName;
import com.datasectech.queryanalyzer.core.query.sensitivity.TraversalContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectAnalyzer extends RelNodeAnalyzer {

    public ProjectAnalyzer(TraversalContext traversalContext) {
        super(traversalContext);
    }

    public void analyze(RelNode rel, List<Pair<String, Object>> values, List<RelNode> inputs) {
        // TODO - check possible case of multiple inputs;

        RelNode input = inputs.get(0);
        SensitivityStatistics inputStats = nodeStats.get(input);

        Map<String, String> sensitiveColumns = projectedSensitiveColumns(rel, input, values);
        nodeStats.put(rel, new SensitivityStatistics(inputStats.selectivity, inputStats.estimatedRows, sensitiveColumns));
    }

    public Map<String, String> projectedSensitiveColumns(RelNode projectNode, RelNode filterNode, List<Pair<String, Object>> values) {

        Map<String, String> sensitiveColumns = new HashMap<>();

        List<TableColumnName> tableColumnNames = new ArrayList<>();

        for (Pair<String, Object> value : values) {

            if (value.right instanceof RexInputRef) {

                RexInputRef inputRef = (RexInputRef) value.right;
                TableColumnName tableColumn = metadataStore.findTableAndColumn(filterNode, inputRef, value.left);

                String sensitiveType = metadataStore.getColumnSensitiveType(tableColumn);

                if (sensitiveType != null) {
                    sensitiveColumns.put(tableColumn.getTableColumnName(), sensitiveType);
                }

                tableColumnNames.add(tableColumn);
            }
        }

        metadataStore.projections.put(projectNode, tableColumnNames);

        return sensitiveColumns;
    }
}
