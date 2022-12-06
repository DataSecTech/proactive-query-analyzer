package com.datasectech.queryanalyzer.core.query.sensitivity.analyzer;

import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.TraversalContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datasectech.queryanalyzer.core.query.SDLRelTraverser.extractValue;

public class JoinAnalyzer extends RelNodeAnalyzer {

    public JoinAnalyzer(TraversalContext traversalContext) {
        super(traversalContext);
    }

    public void analyze(RelNode rel, List<Pair<String, Object>> values, List<RelNode> inputs) {

        Object condition = extractValue("condition", values);
        Object joinType = extractValue("joinType", values);

        if (!(condition instanceof RexCall)) {

            if (condition instanceof RexLiteral) {
                RexLiteral conditionLiteral = (RexLiteral) condition;

                if (conditionLiteral.isAlwaysTrue()) {
                    setAlwaysTrueJoinStats(rel);
                    return;
                }
            }

            throw new RuntimeException("Only RexCall type condition is implemented");
        }

        new JoinRowEstimator(metadataStore, nodeStats, rel, joinType.toString(), (RexCall) condition)
                .calculateAndSetJoinSensitivityStats();
    }

    private void setAlwaysTrueJoinStats(RelNode rel) {

        SensitivityStatistics in1Stat = nodeStats.get(rel.getInput(0));
        SensitivityStatistics in2Stat = nodeStats.get(rel.getInput(1));

        int estimatedRows = in1Stat.estimatedRows * in2Stat.estimatedRows;

        Map<String, String> sensitiveColumns = new HashMap<>(in1Stat.sensitiveColumns);
        sensitiveColumns.putAll(in2Stat.sensitiveColumns);

        nodeStats.put(rel, new SensitivityStatistics(1, estimatedRows, sensitiveColumns));
    }
}
