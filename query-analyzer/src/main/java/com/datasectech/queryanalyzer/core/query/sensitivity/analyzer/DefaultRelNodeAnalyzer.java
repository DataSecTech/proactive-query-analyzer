package com.datasectech.queryanalyzer.core.query.sensitivity.analyzer;

import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.TraversalContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultRelNodeAnalyzer extends RelNodeAnalyzer {

    private final static Logger logger = LogManager.getLogger(DefaultRelNodeAnalyzer.class);

    public DefaultRelNodeAnalyzer(TraversalContext traversalContext) {
        super(traversalContext);
    }

    public void analyze(RelNode rel, List<Pair<String, Object>> values, List<RelNode> inputs) {

        logger.warn("Unknown node type: {}; Adding all the input sensitivity information.", rel.getRelTypeName());

        int estimatedRows = 0;
        Map<String, String> sensitiveColumns = new HashMap<>();

        double maxSensitivity = -1.0;

        for (RelNode input : inputs) {
            SensitivityStatistics inputStats = nodeStats.get(input);

            // Pick height selectivity
            if (inputStats.selectivity > maxSensitivity) {
                maxSensitivity = inputStats.selectivity;
            }

            // Add all estimatedRows
            estimatedRows += inputStats.estimatedRows;

            // Add all sensitive columns
            sensitiveColumns.putAll(inputStats.sensitiveColumns);
        }

        nodeStats.put(rel, new SensitivityStatistics(maxSensitivity, estimatedRows, sensitiveColumns));
    }
}
