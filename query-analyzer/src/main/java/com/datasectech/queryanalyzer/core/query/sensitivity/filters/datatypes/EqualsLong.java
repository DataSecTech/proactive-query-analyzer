package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

public class EqualsLong extends DataTypeAnalyzer {

    @Override
    public double calculateSelectivity(RexInputRef inputRef, RexLiteral literal, ColumnStatistics columnStatistics) {
        long min = Long.parseLong(columnStatistics.min);
        long max = Long.parseLong(columnStatistics.max);

        long value = literal.getValueAs(Long.class);

        if (value < min) {
            return 0.0;
        }

        if (value > max) {
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

    @Override
    public double calculateSelectivity(RexInputRef inputRef1, RexInputRef inputRef2, ColumnStatistics columnStat1, ColumnStatistics columnStat2) {
        if (columnStat1 == columnStat2) {
            // In case comparing same column
            return 1;
        }

        // Assuming that
        // - all the distinct entries are equally distributed over the range
        // - all the distinct entries have equal number of rows

        long min1 = Long.parseLong(columnStat1.min);
        long max1 = Long.parseLong(columnStat1.max);

        long min2 = Long.parseLong(columnStat2.min);
        long max2 = Long.parseLong(columnStat2.max);

        long overlapStart = Math.max(min1, min2);
        long overlapEnd = Math.min(max1, max2);

        if (overlapStart > overlapEnd) {
            // No overlap
            return 0;
        }

        double density1 = (double) columnStat1.distinct / (max1 - min1);
        double density2 = (double) columnStat2.distinct / (max2 - min2);

        long overlap1Start = Math.max(overlapStart, min1);
        long overlap1End = Math.min(overlapEnd, max1);

        long overlap2Start = Math.max(overlapStart, min2);
        long overlap2End = Math.min(overlapEnd, max2);

        double estimated1 = density1 * (overlap1End - overlap1Start);
        double estimated2 = density2 * (overlap2End - overlap2Start);

        if (estimated1 > estimated2) {
            return estimated1 / columnStat1.distinct;
        }

        return estimated2 / columnStat2.distinct;
    }
}
