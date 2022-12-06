package com.datasectech.queryanalyzer.core.query.sensitivity.filters;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.CalciteMetadataStore;
import com.datasectech.queryanalyzer.core.query.sensitivity.analyzer.FilterAnalyzer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

public class NotNullAnalyzer extends OperationAnalyzer {

    public NotNullAnalyzer(CalciteMetadataStore metadataStore, FilterAnalyzer filterAnalyzer) {
        super(metadataStore, filterAnalyzer);
    }

    @Override
    public double calculateSelectivity(RexCall rexCall, RelNode filterNode) {

        if (rexCall.operands.size() != 1) {
            throw new RuntimeException("IS_NOT_NULL on single operand is supported.");
        }

        RexNode operand = rexCall.operands.get(0);

        while (operand instanceof RexCall) {
            operand = ((RexCall) operand).operands.get(0);
        }

        if (operand instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) operand;

            ColumnStatistics columnStatistics = metadataStore.getColumnStatistics(filterNode, inputRef);
            TableStatistics tableStatistics = metadataStore.getTableStatistics(filterNode, inputRef);

            if (columnStatistics.notNull == 0) {
                return 0.0;
            }

            return (double) columnStatistics.notNull / tableStatistics.totalRows;
        }

        throw new RuntimeException("Operation is not handled for operand type: " + operand.getKind());
    }
}
