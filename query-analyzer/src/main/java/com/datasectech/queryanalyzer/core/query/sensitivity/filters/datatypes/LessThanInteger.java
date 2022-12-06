package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

public class LessThanInteger extends DataTypeAnalyzer {

    @Override
    public double calculateSelectivity(RexInputRef inputRef, RexLiteral literal, ColumnStatistics columnStatistics) {
        int min = Integer.parseInt(columnStatistics.min);
        int max = Integer.parseInt(columnStatistics.max);

        int value = literal.getValueAs(Integer.class);

        if (value < min) {
            return 0.0;
        }

        if (value > max) {
            return 1.0;
        }

        return (double) (value - min) / (max - min);
    }

    @Override
    public double calculateSelectivity(RexLiteral literal, RexInputRef inputRef, ColumnStatistics columnStatistics) {
        return 1 - calculateSelectivity(inputRef, literal, columnStatistics);
    }

    @Override
    public double calculateSelectivity(RexLiteral literal1, RexLiteral literal2) {
        return (literal1.getValueAs(Integer.class).compareTo(literal2.getValueAs(Integer.class)) < 0) ? 1.0 : 0.0;
    }

    @Override
    public double calculateSelectivity(RexInputRef inputRef1, RexInputRef inputRef2, ColumnStatistics columnStat1, ColumnStatistics columnStat2) {
        int min1 = Integer.parseInt(columnStat1.min);
        int max1 = Integer.parseInt(columnStat1.max);

        int min2 = Integer.parseInt(columnStat2.min);
        int max2 = Integer.parseInt(columnStat2.max);

        if (max1 < min2) {
            return 1.0;
        }
        if (min1 >= max2) {
            return 0.0;
        }

        return 1.0 / 3.0;
    }
}
