package com.datasectech.queryanalyzer.core.query.sensitivity.filters;

import com.datasectech.queryanalyzer.core.query.sensitivity.CalciteMetadataStore;
import com.datasectech.queryanalyzer.core.query.sensitivity.analyzer.FilterAnalyzer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

public class AndAnalyzer extends OperationAnalyzer {

    public AndAnalyzer(CalciteMetadataStore metadataStore, FilterAnalyzer filterAnalyzer) {
        super(metadataStore, filterAnalyzer);
    }

    @Override
    public double calculateSelectivity(RexCall rexCall, RelNode filterNode) {
        double selectivity = 1;

        for (RexNode operand : rexCall.operands) {

            if (!(operand instanceof RexCall)) {
                throw new RuntimeException("Invalid operand in AND");
            }

            selectivity *= filterAnalyzer.calculateSelectivity((RexCall) operand, filterNode);
        }

        return selectivity;
    }
}
