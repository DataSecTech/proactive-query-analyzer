package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

public class LessThanLong extends DataTypeAnalyzer {
    @Override
    public double calculateSelectivity(RexInputRef inputRef, RexLiteral literal, ColumnStatistics columnStatistics) {
        long min = Long.parseLong(columnStatistics.min);
        long max = Long.parseLong(columnStatistics.max);

        long value = literal.getValueAs(Long.class);

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
        return (literal1.getValueAs(Long.class).compareTo(literal2.getValueAs(Long.class)) < 0) ? 1.0 : 0.0;
    }

    @Override
    public double calculateSelectivity(RexInputRef inputRef1, RexInputRef inputRef2, ColumnStatistics columnStat1, ColumnStatistics columnStat2) {
        long min1 = Long.parseLong(columnStat1.min);
        long max1 = Long.parseLong(columnStat1.max);

        long min2 = Long.parseLong(columnStat2.min);
        long max2 = Long.parseLong(columnStat2.max);

        if (max1 < min2) {
            return 1.0;
        }
        if (min1 >= max2) {
            return 0.0;
        }

        return 1.0 / 3.0;
    }
}
