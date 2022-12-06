package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

public class EqualsIntegerTest {

    protected EqualsInteger equalsInteger;
    protected static RelBuilder builder;
    protected static ColumnStatistics columnStatDeptNo;

    @BeforeClass
    public static void setBuilder() throws SQLException {
        builder = CalciteTestUtils.getRelBuilder();

        columnStatDeptNo = new ColumnStatistics();
        columnStatDeptNo.name = "DEPTNO";
        columnStatDeptNo.dataType = "INTEGER";
        columnStatDeptNo.distinct = 3;
        columnStatDeptNo.min = "10";
        columnStatDeptNo.max = "40";
    }

    @Before
    public void setUp() {
        equalsInteger = new EqualsInteger();
    }

    @Test
    public void inputRefLiteralSelectivityTest() {
        RexInputRef inputRef = builder.scan("EMPS").field("DEPTNO");

        RexLiteral literal1 = (RexLiteral) builder.literal(30);
        RexLiteral literal2 = (RexLiteral) builder.literal(5);
        RexLiteral literal3 = (RexLiteral) builder.literal(50);

        double sensitivity1 = equalsInteger.calculateSelectivity(inputRef, literal1, columnStatDeptNo);
        Assert.assertEquals((double) 1 / columnStatDeptNo.distinct, sensitivity1, CalciteTestUtils.DELTA);

        double sensitivity2 = equalsInteger.calculateSelectivity(inputRef, literal2, columnStatDeptNo);
        Assert.assertEquals(0.0, sensitivity2, CalciteTestUtils.DELTA);

        double sensitivity3 = equalsInteger.calculateSelectivity(inputRef, literal3, columnStatDeptNo);
        Assert.assertEquals(0.0, sensitivity3, CalciteTestUtils.DELTA);
    }

    @Test
    public void literalInputRefSelectivityTest() {
        RexInputRef inputRef = builder.scan("EMPS").field("DEPTNO");

        RexLiteral literal1 = (RexLiteral) builder.literal(30);
        RexLiteral literal2 = (RexLiteral) builder.literal(5);
        RexLiteral literal3 = (RexLiteral) builder.literal(50);

        double sensitivity1 = equalsInteger.calculateSelectivity(literal1, inputRef, columnStatDeptNo);
        Assert.assertEquals((double) 1 / columnStatDeptNo.distinct, sensitivity1, CalciteTestUtils.DELTA);

        double sensitivity2 = equalsInteger.calculateSelectivity(literal2, inputRef, columnStatDeptNo);
        Assert.assertEquals(0.0, sensitivity2, CalciteTestUtils.DELTA);

        double sensitivity3 = equalsInteger.calculateSelectivity(literal3, inputRef, columnStatDeptNo);
        Assert.assertEquals(0.0, sensitivity3, CalciteTestUtils.DELTA);
    }

    @Test
    public void literalLiteralSelectivityTest() {
        RexLiteral a = (RexLiteral) builder.literal(40);
        RexLiteral b = (RexLiteral) builder.literal(40);
        RexLiteral c = (RexLiteral) builder.literal(50);

        double sensitivity = equalsInteger.calculateSelectivity(a, b);
        Assert.assertEquals(1.0, sensitivity, CalciteTestUtils.DELTA);

        double sensitivity2 = equalsInteger.calculateSelectivity(a, c);
        Assert.assertEquals(0.0, sensitivity2, CalciteTestUtils.DELTA);
    }

    @Test
    public void inputRefInputRefSelectivityTest() {
        RexInputRef inputRef1 = builder.scan("EMPS").field("DEPTNO");
        RexInputRef inputRef2 = builder.scan("EMPS").field("EMPID");
        RexInputRef inputRef3 = builder.scan("EMPS").field("EMPNO");

        ColumnStatistics columnStatEmpId = new ColumnStatistics();
        columnStatEmpId.name = "EMPID";
        columnStatEmpId.dataType = "INTEGER";
        columnStatEmpId.distinct = 4;
        columnStatEmpId.min = "1";
        columnStatEmpId.max = "30";

        ColumnStatistics columnStatEmpNo = new ColumnStatistics();
        columnStatEmpNo.name = "EMPNO";
        columnStatEmpNo.dataType = "INTEGER";
        columnStatEmpNo.distinct = 4;
        columnStatEmpNo.min = "100";
        columnStatEmpNo.max = "130";

        double sensitivity1 = equalsInteger.calculateSelectivity(inputRef1, inputRef1, columnStatDeptNo, columnStatDeptNo);
        Assert.assertEquals(1.0, sensitivity1, CalciteTestUtils.DELTA);

        double sensitivity2 = equalsInteger.calculateSelectivity(inputRef2, inputRef3, columnStatEmpId, columnStatEmpNo);
        Assert.assertEquals(0.0, sensitivity2, CalciteTestUtils.DELTA);

        double sensitivity3 = equalsInteger.calculateSelectivity(inputRef1, inputRef2, columnStatDeptNo, columnStatEmpId);
        Assert.assertEquals(0.6896551724137931, sensitivity3, CalciteTestUtils.DELTA);
    }
}
