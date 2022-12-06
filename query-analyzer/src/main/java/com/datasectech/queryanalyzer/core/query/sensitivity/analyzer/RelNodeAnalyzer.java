package com.datasectech.queryanalyzer.core.query.sensitivity.analyzer;

import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.CalciteMetadataStore;
import com.datasectech.queryanalyzer.core.query.sensitivity.TraversalContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.Deque;
import java.util.List;
import java.util.Map;

public abstract class RelNodeAnalyzer {

    protected final CalciteMetadataStore metadataStore;
    protected final Map<RelNode, SensitivityStatistics> nodeStats;
    protected final Deque<RelNode> nodeParentPath;

    public RelNodeAnalyzer(TraversalContext traversalContext) {
        metadataStore = traversalContext.metadataStore;
        nodeStats = traversalContext.nodeStats;
        nodeParentPath = traversalContext.nodeParentPath;
    }

    abstract public void analyze(RelNode rel, List<Pair<String, Object>> values, List<RelNode> inputs);
}
