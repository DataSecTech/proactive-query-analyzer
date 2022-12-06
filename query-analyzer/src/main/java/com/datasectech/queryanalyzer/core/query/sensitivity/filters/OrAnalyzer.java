package com.datasectech.queryanalyzer.core.query.sensitivity.filters;

import com.datasectech.queryanalyzer.core.query.sensitivity.CalciteMetadataStore;
import com.datasectech.queryanalyzer.core.query.sensitivity.analyzer.FilterAnalyzer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

public class OrAnalyzer extends OperationAnalyzer {

    public OrAnalyzer(CalciteMetadataStore metadataStore, FilterAnalyzer filterAnalyzer) {
        super(metadataStore, filterAnalyzer);
    }

    @Override
    public double calculateSelectivity(RexCall rexCall, RelNode filterNode) {

        double selectivity = 0;

        for (RexNode operand : rexCall.operands) {

            if (!(operand instanceof RexCall)) {
                throw new RuntimeException("Invalid operand in OR");
            }

            double current = filterAnalyzer.calculateSelectivity((RexCall) operand, filterNode);

            if (current == 1) {
                return 1; // Short circuit
            }

            if (selectivity == 0) {
                // First one or after sequence of false conditions
                selectivity = current;
            } else {
                selectivity = selectivity + current - (selectivity * current);
            }
        }

        return selectivity;
    }
}
