package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

public class LessThanDouble extends DataTypeAnalyzer {

    @Override
    public double calculateSelectivity(RexInputRef inputRef, RexLiteral literal, ColumnStatistics columnStatistics) {
        double min = Double.parseDouble(columnStatistics.min);
        double max = Double.parseDouble(columnStatistics.max);

        double value = literal.getValueAs(Double.class);

        if (value < min) {
            return 0.0;
        }

        if (value > max) {
            return 1.0;
        }

        return (value - min) / (max - min);
    }

    @Override
    public double calculateSelectivity(RexLiteral literal, RexInputRef inputRef, ColumnStatistics columnStatistics) {
        return 1 - calculateSelectivity(inputRef, literal, columnStatistics);
    }

    @Override
    public double calculateSelectivity(RexLiteral literal1, RexLiteral literal2) {
        return (literal1.getValueAs(Double.class).compareTo(literal2.getValueAs(Double.class)) < 0) ? 1.0 : 0.0;
    }

    @Override
    public double calculateSelectivity(RexInputRef inputRef1, RexInputRef inputRef2, ColumnStatistics columnStat1, ColumnStatistics columnStat2) {
        double min1 = Double.parseDouble(columnStat1.min);
        double max1 = Double.parseDouble(columnStat1.max);

        double min2 = Double.parseDouble(columnStat2.min);
        double max2 = Double.parseDouble(columnStat2.max);

        if (max1 < min2) {
            return 1.0;
        }
        if (min1 >= max2) {
            return 0.0;
        }

        return 1.0 / 3.0;

    }
}
