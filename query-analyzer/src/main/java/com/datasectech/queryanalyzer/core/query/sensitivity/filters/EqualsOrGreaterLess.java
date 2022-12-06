package com.datasectech.queryanalyzer.core.query.sensitivity.filters;

import com.datasectech.queryanalyzer.core.query.sensitivity.CalciteMetadataStore;
import com.datasectech.queryanalyzer.core.query.sensitivity.analyzer.FilterAnalyzer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EqualsOrGreaterLess extends OperationAnalyzer {

    private final static Logger logger = LogManager.getLogger(EqualsOrGreaterLess.class);

    protected final EqualsLessThanAnalyzer equalsAnalyzer;
    protected final OperationAnalyzer unequalsAnalyzer;

    public EqualsOrGreaterLess(CalciteMetadataStore metadataStore, FilterAnalyzer filterAnalyzer, SqlKind sqlKind) {
        super(metadataStore, filterAnalyzer);

        equalsAnalyzer = new EqualsLessThanAnalyzer(metadataStore, filterAnalyzer, SqlKind.EQUALS);

        if (SqlKind.GREATER_THAN.equals(sqlKind)) {
            unequalsAnalyzer = new GreaterThanAnalyzer(metadataStore, filterAnalyzer);

        } else if (SqlKind.LESS_THAN.equals(sqlKind)) {
            unequalsAnalyzer = new EqualsLessThanAnalyzer(metadataStore, filterAnalyzer, SqlKind.LESS_THAN);

        } else {
            throw new RuntimeException("Only LessThan and GreaterThan ");
        }
    }

    protected double calculateUnequalsSelectivity(RexCall rexCall, RelNode filterNode) {
        return unequalsAnalyzer.calculateSelectivity(rexCall, filterNode);
    }

    @Override
    public double calculateSelectivity(RexCall rexCall, RelNode filterNode) {

        double equalsSelectivity = equalsAnalyzer.calculateSelectivity(rexCall, filterNode);
        double unequalsSelectivity = calculateUnequalsSelectivity(rexCall, filterNode);

        if (equalsSelectivity == 1.0 || unequalsSelectivity == 1.0) {
            return 1.0;
        }

        double selectivity = equalsSelectivity + unequalsSelectivity - (equalsSelectivity * unequalsSelectivity);

        if (selectivity <= 0) {
            logger.debug("Found sensitivity less than zero ({}), for RexCall {}, on FilterNode {}", selectivity, rexCall, filterNode);
            return 0;
        }

        if (selectivity <= 1.0) {
            return selectivity;
        }

        logger.debug("Found sensitivity greater than one ({}), for RexCall {}, on FilterNode {}", selectivity, rexCall, filterNode);
        return 1;
    }
}
