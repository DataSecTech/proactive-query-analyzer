package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

import java.sql.Date;

public class LessThanDate extends DataTypeAnalyzer {

    @Override
    public double calculateSelectivity(RexInputRef inputRef, RexLiteral literal, ColumnStatistics columnStatistics) {
        Date min = Date.valueOf(columnStatistics.min);
        Date max = Date.valueOf(columnStatistics.max);

        Date value = EqualsDate.parseDateLiteral(literal);

        if (value.compareTo(min) < 0) {
            return 0.0;
        }
        if (value.compareTo(max) > 0) {
            return 1.0;
        }

        long diffValueMinMiliSec = Math.abs(value.getTime() - min.getTime());
        long diffMaxMinMiliSec = Math.abs(max.getTime() - min.getTime());

        return (double) diffValueMinMiliSec / diffMaxMinMiliSec;
    }

    @Override
    public double calculateSelectivity(RexLiteral literal, RexInputRef inputRef, ColumnStatistics columnStatistics) {
        return 1 - calculateSelectivity(inputRef, literal, columnStatistics);
    }

    @Override
    public double calculateSelectivity(RexLiteral literal1, RexLiteral literal2) {
        Date date1 = Date.valueOf(literal1.getValueAs(String.class));
        Date date2 = Date.valueOf(literal2.getValueAs(String.class));

        return (date1.compareTo(date2) < 0) ? 1.0 : 0.0;
    }

    @Override
    public double calculateSelectivity(RexInputRef inputRef1, RexInputRef inputRef2, ColumnStatistics columnStat1, ColumnStatistics columnStat2) {
        Date min1 = Date.valueOf(columnStat1.min);
        Date max1 = Date.valueOf(columnStat1.max);

        Date min2 = Date.valueOf(columnStat2.min);
        Date max2 = Date.valueOf(columnStat2.max);

        if (max1.compareTo(min2) < 0) {
            return 1.0;
        }
        if (min1.compareTo(max2) >= 0) {
            return 0.0;
        }

        return 1.0 / 3.0;
    }
}
