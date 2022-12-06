package com.datasectech.queryanalyzer.core.query.sensitivity.filters;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.CalciteMetadataStore;
import com.datasectech.queryanalyzer.core.query.sensitivity.analyzer.FilterAnalyzer;
import com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.HashMap;
import java.util.Map;

public class GreaterThanAnalyzer extends OperationAnalyzer {

    protected final Map<String, DataTypeAnalyzer> typeAnalyzer;

    public GreaterThanAnalyzer(CalciteMetadataStore metadataStore, FilterAnalyzer filterAnalyzer) {
        super(metadataStore, filterAnalyzer);

        typeAnalyzer = new HashMap<>();

        typeAnalyzer.put("INTEGER", new LessThanInteger());
        typeAnalyzer.put("LONG", new LessThanLong());
        typeAnalyzer.put("FLOAT", new LessThanDouble());
        typeAnalyzer.put("DOUBLE", new LessThanDouble());
        typeAnalyzer.put("DECIMAL", new LessThanDecimal());
        typeAnalyzer.put("DATE", new LessThanDate());
        typeAnalyzer.put("CHAR", new LessThanString());
        typeAnalyzer.put("VARCHAR", new LessThanString());
    }

    @Override
    public double calculateSelectivity(RexCall rexCall, RelNode filterNode) {
        if (rexCall.operands.size() != 2) {
            throw new RuntimeException("Greater than on two operands are supported.");
        }

        RexNode operand1 = rexCall.operands.get(0);
        RexNode operand2 = rexCall.operands.get(1);

        while (operand1 instanceof RexCall) {
            operand1 = ((RexCall) operand1).operands.get(0);
        }

        while (operand2 instanceof RexCall) {
            operand2 = ((RexCall) operand2).operands.get(0);
        }

        if (operand1 instanceof RexInputRef && operand2 instanceof RexLiteral) {
            RexInputRef inputRef = (RexInputRef) operand1;
            String typeName = inputRef.getType().toString();

            ColumnStatistics columnStatistics = metadataStore.getColumnStatistics(filterNode, inputRef);

            return typeAnalyzer.get(typeName).calculateSelectivity((RexLiteral) operand2, inputRef, columnStatistics);

        } else if (operand1 instanceof RexLiteral && operand2 instanceof RexInputRef) {

            RexInputRef inputRef = (RexInputRef) operand2;
            String typeName = inputRef.getType().toString();

            ColumnStatistics columnStatistics = metadataStore.getColumnStatistics(filterNode, inputRef);

            return typeAnalyzer.get(typeName).calculateSelectivity(inputRef, (RexLiteral) operand1, columnStatistics);

        } else if (operand1 instanceof RexLiteral && operand2 instanceof RexLiteral) {

            RexLiteral literal = (RexLiteral) operand1;
            String typeName = literal.getTypeName().toString();

            return typeAnalyzer.get(typeName).calculateSelectivity((RexLiteral) operand2, (RexLiteral) operand1);

        } else if (operand1 instanceof RexInputRef && operand2 instanceof RexInputRef) {

            RexInputRef inputRef1 = (RexInputRef) operand1;
            RexInputRef inputRef2 = (RexInputRef) operand2;

            ColumnStatistics columnStat1 = metadataStore.getColumnStatistics(filterNode, inputRef1);
            ColumnStatistics columnStat2 = metadataStore.getColumnStatistics(filterNode, inputRef2);

            String typeName = inputRef1.getType().toString();

            return typeAnalyzer.get(typeName).calculateSelectivity(inputRef2, inputRef1, columnStat2, columnStat1);
        }

        throw new RuntimeException("Unknown operand combination of " + operand1.getKind()
                + " and " + operand2.getKind() + " in GreaterThanAnalyzer"
        );
    }
}
