package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

import java.math.BigDecimal;

public class EqualsDecimal extends DataTypeAnalyzer {

    @Override
    public double calculateSelectivity(RexInputRef inputRef, RexLiteral literal, ColumnStatistics columnStatistics) {
        BigDecimal min = new BigDecimal(columnStatistics.min);
        BigDecimal max = new BigDecimal(columnStatistics.max);

        BigDecimal value = literal.getValueAs(BigDecimal.class);

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

    private BigDecimal maxDecimal(BigDecimal bigDecimal1, BigDecimal bigDecimal2) {
        if (bigDecimal1.compareTo(bigDecimal2) > 0) {
            return bigDecimal1;
        } else {
            return bigDecimal2;
        }
    }

    private BigDecimal minDecimal(BigDecimal bigDecimal1, BigDecimal bigDecimal2) {
        if (bigDecimal1.compareTo(bigDecimal2) < 0) {
            return bigDecimal1;
        } else {
            return bigDecimal2;
        }
    }

    @Override
    public double calculateSelectivity(RexInputRef inputRef1, RexInputRef inputRef2, ColumnStatistics columnStat1, ColumnStatistics columnStat2) {
        //need to implement this

        if (columnStat1 == columnStat2) {
            // In case comparing same column
            return 1;
        }

        BigDecimal min1 = new BigDecimal(columnStat1.min);
        BigDecimal max1 = new BigDecimal(columnStat1.max);

        BigDecimal min2 = new BigDecimal(columnStat2.min);
        BigDecimal max2 = new BigDecimal(columnStat2.max);

        BigDecimal overlapStart = maxDecimal(min1, min2);
        BigDecimal overlapEnd = minDecimal(max1, max2);

        if (overlapStart.compareTo(overlapEnd) > 0) {
            // No overlap
            return 0;
        }

        double density1 = (double) columnStat1.distinct / max1.subtract(min1).doubleValue();
        double density2 = (double) columnStat2.distinct / max2.subtract(min2).doubleValue();

        BigDecimal overlap1Start = maxDecimal(overlapStart, min1);
        BigDecimal overlap1End = minDecimal(overlapEnd, max1);

        BigDecimal overlap2Start = maxDecimal(overlapStart, min2);
        BigDecimal overlap2End = minDecimal(overlapEnd, max2);

        double estimated1 = density1 * (overlap1End.subtract(overlap1Start).doubleValue());
        double estimated2 = density2 * (overlap2End.subtract(overlap2Start).doubleValue());

        if (estimated1 > estimated2) {
            return estimated1 / columnStat1.distinct;
        }

        return estimated2 / columnStat2.distinct;
    }
}
