package com.datasectech.queryanalyzer.core.query.sensitivity;

import com.datasectech.queryanalyzer.core.query.SDLRelTraverser;
import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.analyzer.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SensitivityAnalyzer extends SDLRelTraverser {

    private final static Logger logger = LogManager.getLogger(SensitivityAnalyzer.class);

    // Need better way to instantiate and organize attributes

    protected SensitivityStatistics sensitivityStat;

    protected DefaultRelNodeAnalyzer defaultNodeAnalyzer;
    protected final Map<String, RelNodeAnalyzer> nodeAnalyzers;

    protected final CalciteMetadataStore metadataStore;
    protected final Map<RelNode, SensitivityStatistics> nodeStats;
    protected final TraversalContext traversalContext;

    public SensitivityAnalyzer(PrintWriter pw, CalciteMetadataStore metadataStore) {
        super(pw);

        this.metadataStore = metadataStore;

        nodeStats = new HashMap<>();
        nodeAnalyzers = new HashMap<>();

        traversalContext = new TraversalContext(metadataStore, nodeStats, nodeParentPath);

        initializeMetadataStore();
        initializeNodeAnalyzers();
    }

    public void initializeNodeAnalyzers() {

        nodeAnalyzers.put("LogicalTableScan", new TableScanAnalyzer(traversalContext));
        nodeAnalyzers.put("LogicalProject", new ProjectAnalyzer(traversalContext));
        nodeAnalyzers.put("LogicalFilter", new FilterAnalyzer(traversalContext));
        nodeAnalyzers.put("LogicalJoin", new JoinAnalyzer(traversalContext));
        nodeAnalyzers.put("LogicalAggregate", new AggregateAnalyzer(traversalContext));

        defaultNodeAnalyzer = new DefaultRelNodeAnalyzer(traversalContext);
    }

    public void initializeMetadataStore() {
        metadataStore.buildTableStatistics();
    }

    public SensitivityStatistics analyze(RelNode rel) {
        rel.explain(this);

        return sensitivityStat;
    }

    @Override
    protected void explain_(RelNode rel, List<Pair<String, Object>> values) {

        final List<RelNode> inputs = extractAndExplainInputs(rel, values);

        RelNodeAnalyzer relNodeAnalyzer = nodeAnalyzers.getOrDefault(rel.getRelTypeName(), defaultNodeAnalyzer);
        relNodeAnalyzer.analyze(rel, values, inputs);

        if (nodeParentPath.size() == 0) {
            if (!nodeStats.containsKey(rel)) {
                logger.error("Unknown root node type: " + rel.getRelTypeName());
                sensitivityStat = null;
                return;
            }

            sensitivityStat = nodeStats.get(rel);
        }
    }
}
