package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.Bucket;
import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LessThanString extends DataTypeAnalyzer {

    @Override
    public double calculateSelectivity(RexInputRef inputRef, RexLiteral literal, ColumnStatistics columnStatistics) {

        String min = columnStatistics.min.toLowerCase();
        String max = columnStatistics.max.toLowerCase();

        String value = literal.getValueAs(String.class).toLowerCase();

        if (value.compareTo(min) < 0) {
            return 0.0;
        }

        if (value.compareTo(max) > 0) {
            return 1.0;
        }

        String key = String.valueOf(value.charAt(0));

        Map<String, Bucket> bucketMap = columnStatistics.histogram.bucketMap;

        List<String> keyList = new ArrayList<>(bucketMap.keySet());
        Collections.sort(keyList);

        int cumulativeFreq = 0;

        for (String keyValue : keyList) {
            if (keyValue.compareTo(key) < 0) {
                cumulativeFreq += bucketMap.get(keyValue).noOfItems;
            } else {
                break;
            }
        }

        return (double) cumulativeFreq / columnStatistics.notNull;
    }

    @Override
    public double calculateSelectivity(RexLiteral literal, RexInputRef inputRef, ColumnStatistics columnStatistics) {
        return 1 - calculateSelectivity(inputRef, literal, columnStatistics);
    }

    @Override
    public double calculateSelectivity(RexLiteral literal1, RexLiteral literal2) {
        return (literal1.getValueAs(String.class).compareTo(literal2.getValueAs(String.class)) < 0) ? 1.0 : 0.0;
    }

    @Override
    public double calculateSelectivity(RexInputRef inputRef1, RexInputRef inputRef2, ColumnStatistics columnStat1, ColumnStatistics columnStat2) {
        String min1 = columnStat1.min.toLowerCase();
        String max1 = columnStat1.max.toLowerCase();

        String min2 = columnStat2.min.toLowerCase();
        String max2 = columnStat2.max.toLowerCase();

        if (max1.compareTo(min2) < 0) {
            return 1.0;
        }
        if (min1.compareTo(max2) >= 0) {
            return 0.0;
        }

        return 1.0 / 3.0;

    }
}
