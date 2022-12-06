package com.datasectech.queryanalyzer.core.query.sensitivity.filters;

import com.datasectech.queryanalyzer.core.query.sensitivity.CalciteMetadataStore;
import com.datasectech.queryanalyzer.core.query.sensitivity.analyzer.FilterAnalyzer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;

public class NotEqualsAnalyzer extends EqualsLessThanAnalyzer {

    public NotEqualsAnalyzer(CalciteMetadataStore metadataStore, FilterAnalyzer filterAnalyzer) {
        super(metadataStore, filterAnalyzer, SqlKind.EQUALS);
    }

    @Override
    public double calculateSelectivity(RexCall rexCall, RelNode filterNode) {
        return 1.0 - super.calculateSelectivity(rexCall, filterNode);
    }
}
