package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class LessThanDecimal extends DataTypeAnalyzer {

    @Override
    public double calculateSelectivity(RexInputRef inputRef, RexLiteral literal, ColumnStatistics columnStatistics) {
        BigDecimal min = new BigDecimal(columnStatistics.min);
        BigDecimal max = new BigDecimal(columnStatistics.max);

        BigDecimal value = literal.getValueAs(BigDecimal.class);

        if (value.compareTo(min) < 0) {
            return 0.0;
        }

        if (value.compareTo(max) > 0) {
            return 1.0;
        }

        BigDecimal diffValueMin = value.subtract(min);
        BigDecimal diffMaxMin = max.subtract(min);

        BigDecimal result = diffValueMin.divide(diffMaxMin, RoundingMode.CEILING);

        return result.doubleValue();
    }

    @Override
    public double calculateSelectivity(RexLiteral literal, RexInputRef inputRef, ColumnStatistics columnStatistics) {
        return 1 - calculateSelectivity(inputRef, literal, columnStatistics);
    }

    @Override
    public double calculateSelectivity(RexLiteral literal1, RexLiteral literal2) {
        return (literal1.getValueAs(BigDecimal.class).compareTo(literal2.getValueAs(BigDecimal.class)) < 0) ? 1.0 : 0.0;
    }

    @Override
    public double calculateSelectivity(RexInputRef inputRef1, RexInputRef inputRef2, ColumnStatistics columnStat1, ColumnStatistics columnStat2) {
        BigDecimal min1 = new BigDecimal(columnStat1.min);
        BigDecimal max1 = new BigDecimal(columnStat1.max);

        BigDecimal min2 = new BigDecimal(columnStat2.min);
        BigDecimal max2 = new BigDecimal(columnStat2.max);

        if (max1.compareTo(min2) < 0) {
            return 1.0;
        }
        if (min1.compareTo(max2) >= 0) {
            return 0.0;
        }

        return 1.0 / 3.0;
    }
}
