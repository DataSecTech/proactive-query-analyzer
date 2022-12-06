package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

import java.sql.Date;
import java.util.GregorianCalendar;

public class EqualsDate extends DataTypeAnalyzer {

    public static Date parseDateLiteral(RexLiteral literal) {

        if (literal.getValue() instanceof GregorianCalendar) {
            GregorianCalendar gregorianCalendar = (GregorianCalendar) literal.getValue();
            return new Date(gregorianCalendar.getTimeInMillis());
        }

        return Date.valueOf(literal.getValueAs(String.class));
    }

    @Override
    public double calculateSelectivity(RexInputRef inputRef, RexLiteral literal, ColumnStatistics columnStatistics) {
        Date min = Date.valueOf(columnStatistics.min);
        Date max = Date.valueOf(columnStatistics.max);

        Date value = parseDateLiteral(literal);

        if (value.compareTo(min) < 0) {
            return 0.0;
        }

        if (value.compareTo(max) > 0) {
            return 0.0;
        }

        return 1.0 / (double) columnStatistics.distinct;
    }

    @Override
    public double calculateSelectivity(RexLiteral literal, RexInputRef inputRef, ColumnStatistics columnStatistics) {
        return calculateSelectivity(inputRef, literal, columnStatistics);
    }

    @Override
    public double calculateSelectivity(RexLiteral literal1, RexLiteral literal2) {
        return literal1.getValue().equals(literal2.getValue()) ? 1.0 : 0.0;
    }

    private Date maxDate(Date date1, Date date2) {
        if (date1.compareTo(date2) > 0) {
            return date1;
        } else {
            return date2;
        }
    }

    private Date minDate(Date date1, Date date2) {
        if (date1.compareTo(date2) < 0) {
            return date1;
        } else {
            return date2;
        }
    }

    @Override
    public double calculateSelectivity(RexInputRef inputRef1, RexInputRef inputRef2, ColumnStatistics columnStat1, ColumnStatistics columnStat2) {

        if (columnStat1 == columnStat2) {
            // In case comparing same column
            return 1;
        }

        Date min1 = Date.valueOf(columnStat1.min);
        Date max1 = Date.valueOf(columnStat1.max);

        Date min2 = Date.valueOf(columnStat2.min);
        Date max2 = Date.valueOf(columnStat2.max);

        Date overlapStart = maxDate(min1, min2);
        Date overlapEnd = minDate(max1, max2);

        if (overlapStart.compareTo(overlapEnd) > 0) {
            // No overlap
            return 0;
        }

        double density1 = (double) columnStat1.distinct / Math.abs(max1.getTime() - min1.getTime());
        double density2 = (double) columnStat2.distinct / Math.abs(max2.getTime() - min2.getTime());

        Date overlap1Start = maxDate(overlapStart, min1);
        Date overlap1End = minDate(overlapEnd, max1);

        Date overlap2Start = maxDate(overlapStart, min2);
        Date overlap2End = minDate(overlapEnd, max2);

        double estimated1 = density1 * (Math.abs(overlap1End.getTime() - overlap1Start.getTime()));
        double estimated2 = density2 * (Math.abs(overlap2End.getTime() - Math.abs(overlap2Start.getTime())));

        if (estimated1 > estimated2) {
            return estimated1 / columnStat1.distinct;
        }

        return estimated2 / columnStat2.distinct;
    }
}
