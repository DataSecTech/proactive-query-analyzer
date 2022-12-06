package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.Bucket;
import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

import java.util.Map;

public class EqualsString extends DataTypeAnalyzer {

    @Override
    public double calculateSelectivity(RexInputRef inputRef, RexLiteral literal, ColumnStatistics columnStatistics) {
        String min = columnStatistics.min.toLowerCase();
        String max = columnStatistics.max.toLowerCase();

        String value = literal.getValueAs(String.class).toLowerCase();

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

    @Override
    public double calculateSelectivity(RexInputRef inputRef1, RexInputRef inputRef2, ColumnStatistics columnStat1, ColumnStatistics columnStat2) {
        if (columnStat1 == columnStat2) {
            return 1.0;
        }

        Map<String, Bucket> bucketMap1 = columnStat1.histogram.bucketMap;
        Map<String, Bucket> bucketMap2 = columnStat2.histogram.bucketMap;

        double overlapCount = 0;

        for (String key : bucketMap1.keySet()) {

            if (bucketMap2.get(key) == null) {
                continue;
            }

            String min1 = bucketMap1.get(key).min;
            String max1 = bucketMap1.get(key).max;
            int noOfItems1 = bucketMap1.get(key).noOfItems;

            String min2 = bucketMap2.get(key).min;
            String max2 = bucketMap2.get(key).max;
            int noOfItems2 = bucketMap2.get(key).noOfItems;

            if (((min2.compareTo(max1) < 0) && (max2.compareTo(max1) >= 0)) ||
                    ((min1.compareTo(max2) < 0) && (max1.compareTo(max2) >= 0))) {
                overlapCount += (double) (noOfItems1 + noOfItems2) / 3;
            }

            if (min1.compareTo(min2) == 0 && max1.compareTo(max2) == 0) {
                overlapCount += noOfItems1 + noOfItems2;
            }
        }

        return overlapCount / Math.max(columnStat1.notNull, columnStat2.notNull);
    }
}
