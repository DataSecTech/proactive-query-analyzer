package com.datasectech.queryanalyzer.core.query.sensitivity.analyzer;

import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.TraversalContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.List;

public class TableScanAnalyzer extends RelNodeAnalyzer {

    public TableScanAnalyzer(TraversalContext traversalContext) {
        super(traversalContext);
    }

    public void analyze(RelNode rel, List<Pair<String, Object>> values, List<RelNode> inputs) {
        List<String> qualifiedName = rel.getTable().getQualifiedName();

        String tableName = qualifiedName.get(1);
        TableStatistics tableStat = metadataStore.schemaStats.get(tableName);

        nodeStats.put(rel, new SensitivityStatistics(1.0, tableStat.totalRows, tableStat.sensitiveColumns));
    }
}
