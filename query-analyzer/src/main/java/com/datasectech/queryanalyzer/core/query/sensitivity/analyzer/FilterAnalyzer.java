package com.datasectech.queryanalyzer.core.query.sensitivity.analyzer;

import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.TraversalContext;
import com.datasectech.queryanalyzer.core.query.sensitivity.filters.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datasectech.queryanalyzer.core.query.SDLRelTraverser.extractValue;

public class FilterAnalyzer extends RelNodeAnalyzer {

    protected final Map<SqlKind, OperationAnalyzer> analyzers;

    public FilterAnalyzer(TraversalContext traversalContext) {
        super(traversalContext);

        analyzers = new HashMap<>();

        analyzers.put(SqlKind.EQUALS, new EqualsLessThanAnalyzer(metadataStore, this, SqlKind.EQUALS));
        analyzers.put(SqlKind.NOT_EQUALS, new NotEqualsAnalyzer(metadataStore, this));
        analyzers.put(SqlKind.LESS_THAN, new EqualsLessThanAnalyzer(metadataStore, this, SqlKind.LESS_THAN));
        analyzers.put(SqlKind.GREATER_THAN, new GreaterThanAnalyzer(metadataStore, this));

        analyzers.put(SqlKind.LESS_THAN_OR_EQUAL, new EqualsOrGreaterLess(metadataStore, this, SqlKind.LESS_THAN));
        analyzers.put(SqlKind.GREATER_THAN_OR_EQUAL, new EqualsOrGreaterLess(metadataStore, this, SqlKind.GREATER_THAN));

        analyzers.put(SqlKind.IS_NOT_NULL, new NotNullAnalyzer(metadataStore, this));

        analyzers.put(SqlKind.AND, new AndAnalyzer(metadataStore, this));
        analyzers.put(SqlKind.OR, new OrAnalyzer(metadataStore, this));
    }

    public double calculateSelectivity(RexCall condition, RelNode filterNode) {
        SqlKind operationKind = condition.op.kind;

        if (!analyzers.containsKey(operationKind)) {
            throw new RuntimeException("Unsupported operation type: " + operationKind);
        }

        return analyzers.get(operationKind).calculateSelectivity(condition, filterNode);
    }

    public void analyze(RelNode rel, List<Pair<String, Object>> values, List<RelNode> inputs) {
        // TODO - handle multiple inputs, (possible?)

        RexCall condition = (RexCall) extractValue("condition", values);
        double selectivity = calculateSelectivity(condition, rel);

        RelNode input = rel.getInput(0);
        int estimatedRows = (int) Math.ceil(selectivity * nodeStats.get(input).estimatedRows);

        nodeStats.put(rel, new SensitivityStatistics(selectivity, estimatedRows, nodeStats.get(input).sensitiveColumns));
    }
}
