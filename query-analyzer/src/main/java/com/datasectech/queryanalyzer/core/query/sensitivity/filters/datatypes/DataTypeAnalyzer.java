package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

public abstract class DataTypeAnalyzer {
    public abstract double calculateSelectivity(RexInputRef inputRef, RexLiteral literal, ColumnStatistics columnStatistics);

    public abstract double calculateSelectivity(RexLiteral literal, RexInputRef inputRef, ColumnStatistics columnStatistics);

    public abstract double calculateSelectivity(RexLiteral literal1, RexLiteral literal2);

    public abstract double calculateSelectivity(RexInputRef inputRef1, RexInputRef inputRef2, ColumnStatistics columnStat1, ColumnStatistics columnStat2);
}
