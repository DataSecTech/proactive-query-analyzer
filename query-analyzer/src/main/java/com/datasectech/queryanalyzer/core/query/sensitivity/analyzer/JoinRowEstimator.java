package com.datasectech.queryanalyzer.core.query.sensitivity.analyzer;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.CalciteMetadataStore;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class JoinRowEstimator {

    private final static Logger logger = LogManager.getLogger(JoinRowEstimator.class);

    protected final CalciteMetadataStore metadataStore;
    protected final Map<RelNode, SensitivityStatistics> nodeStats;

    protected final RelNode rel;
    protected final String joinType;

    protected int denominator;
    protected final SensitivityStatistics in1Stat;
    protected final SensitivityStatistics in2Stat;
    protected final Map<String, String> sensitiveColumns;
    protected final int innerJoinEstimatedRows;

    public JoinRowEstimator(
            CalciteMetadataStore metadataStore, Map<RelNode, SensitivityStatistics> nodeStats,
            RelNode rel, String joinType, RexCall conditionCall
    ) {
        this.metadataStore = metadataStore;
        this.nodeStats = nodeStats;

        this.rel = rel;
        this.joinType = joinType;

        this.denominator = calculateDenominator(conditionCall);
        if (this.denominator < 1) {
            this.denominator = 1;
        }

        this.in1Stat = nodeStats.get(rel.getInput(0));
        this.in2Stat = nodeStats.get(rel.getInput(1));

        sensitiveColumns = new HashMap<>(in1Stat.sensitiveColumns);
        sensitiveColumns.putAll(in2Stat.sensitiveColumns);
        this.innerJoinEstimatedRows = calculateInnerJoinEstimatedRows();
    }

    private int calculateDenominator(RexCall conditionCall) {

        if (SqlKind.EQUALS.equals(conditionCall.op.kind)) {
            return calculateMaxDistinct(conditionCall);
        }

        int maxDenominator = 1;

        if (SqlKind.AND.equals(conditionCall.op.kind)) {

            for (RexNode operand : conditionCall.operands) {

                if (!(operand instanceof RexCall)) {
                    logger.info("AND in join condition has unknown type operand; defaulting to worst case");
                    return 1;
                }

                maxDenominator = Integer.max(maxDenominator, calculateMaxDistinct((RexCall) operand));
            }

            return maxDenominator;
        }

        logger.warn("Unknown type of condition call in Join {}; defaulting to worst case", conditionCall.op);
        return 1;
    }

    private int calculateMaxDistinct(RexCall conditionCall) {

        if (!conditionCall.op.kind.equals(SqlKind.EQUALS) || conditionCall.operands.size() != 2) {
            throw new RuntimeException("Only EQUALS on two input reference is implemented");
        }

        RexNode op1 = conditionCall.operands.get(0);
        RexNode op2 = conditionCall.operands.get(1);

        if (!(op1 instanceof RexInputRef && op2 instanceof RexInputRef)) {
            throw new RuntimeException("Only two input reference is implemented");
        }

        ColumnStatistics columnStat1 = metadataStore.getColumnStatistics(rel, (RexInputRef) op1);
        ColumnStatistics columnStat2 = metadataStore.getColumnStatistics(rel, (RexInputRef) op2);

        return Math.max(columnStat1.distinct, columnStat2.distinct);
    }

    private int calculateInnerJoinEstimatedRows() {
        double totalRows = (double) in1Stat.estimatedRows * in2Stat.estimatedRows;
        return (int) Math.ceil(totalRows / (double) denominator);
    }

    public void calculateAndSetJoinSensitivityStats() {

        if ("inner".equals(joinType)) {
            nodeStats.put(rel, new SensitivityStatistics(1, innerJoinEstimatedRows, sensitiveColumns));
        } else if ("left".equals(joinType)) {
            nodeStats.put(rel, new SensitivityStatistics(1, leftJoinEstimatedRows(), sensitiveColumns));
        } else if ("right".equals(joinType)) {
            nodeStats.put(rel, new SensitivityStatistics(1, rightJoinEstimatedRows(), sensitiveColumns));
        } else if ("full".equals(joinType)) {
            nodeStats.put(rel, new SensitivityStatistics(1, fullJoinEstimatedRows(), sensitiveColumns));
        } else {
            throw new RuntimeException("Unknown join type: " + joinType);
        }
    }

    private int leftJoinEstimatedRows() {
        return Math.max(innerJoinEstimatedRows, in1Stat.estimatedRows);
    }

    private int rightJoinEstimatedRows() {
        return Math.max(innerJoinEstimatedRows, in2Stat.estimatedRows);
    }

    private int fullJoinEstimatedRows() {
        return leftJoinEstimatedRows() + rightJoinEstimatedRows() - innerJoinEstimatedRows;
    }
}
