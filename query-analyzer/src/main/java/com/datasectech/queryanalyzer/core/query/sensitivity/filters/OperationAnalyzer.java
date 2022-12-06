package com.datasectech.queryanalyzer.core.query.sensitivity.filters;

import com.datasectech.queryanalyzer.core.query.sensitivity.CalciteMetadataStore;
import com.datasectech.queryanalyzer.core.query.sensitivity.analyzer.FilterAnalyzer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;

public abstract class OperationAnalyzer {

    protected final CalciteMetadataStore metadataStore;
    protected final FilterAnalyzer filterAnalyzer;

    public OperationAnalyzer(CalciteMetadataStore metadataStore, FilterAnalyzer filterAnalyzer) {
        this.metadataStore = metadataStore;
        this.filterAnalyzer = filterAnalyzer;
    }

    public abstract double calculateSelectivity(RexCall rexCall, RelNode filterNode);
}
