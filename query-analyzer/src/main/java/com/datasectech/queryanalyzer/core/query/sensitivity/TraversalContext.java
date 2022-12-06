package com.datasectech.queryanalyzer.core.query.sensitivity;

import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import org.apache.calcite.rel.RelNode;

import java.util.Deque;
import java.util.Map;

public class TraversalContext {

    public final CalciteMetadataStore metadataStore;
    public final Map<RelNode, SensitivityStatistics> nodeStats;
    public final Deque<RelNode> nodeParentPath;

    public TraversalContext(
            CalciteMetadataStore metadataStore,
            Map<RelNode, SensitivityStatistics> nodeStats,
            Deque<RelNode> nodeParentPath
    ) {
        this.metadataStore = metadataStore;
        this.nodeStats = nodeStats;
        this.nodeParentPath = nodeParentPath;
    }
}
