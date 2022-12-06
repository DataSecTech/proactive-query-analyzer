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

public class LessThanIntegerTest {

    protected LessThanInteger lessThanInteger;
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
        lessThanInteger = new LessThanInteger();
    }

    @Test
    public void inputRefLiteralSelectivityTest() {
        RexInputRef inputRef = builder.scan("EMPS").field("DEPTNO");

        RexLiteral literal1 = (RexLiteral) builder.literal(5);
        RexLiteral literal2 = (RexLiteral) builder.literal(50);
        RexLiteral literal3 = (RexLiteral) builder.literal(25);

        double sensitivity1 = lessThanInteger.calculateSelectivity(inputRef, literal1, columnStatDeptNo);
        Assert.assertEquals(0.0, sensitivity1, CalciteTestUtils.DELTA);

        double sensitivity2 = lessThanInteger.calculateSelectivity(inputRef, literal2, columnStatDeptNo);
        Assert.assertEquals(1.0, sensitivity2, CalciteTestUtils.DELTA);

        double sensitivity3 = lessThanInteger.calculateSelectivity(inputRef, literal3, columnStatDeptNo);
        Assert.assertEquals(0.5, sensitivity3, CalciteTestUtils.DELTA);
    }

    @Test
    public void literalInputRefSelectivityTest() {
        RexInputRef inputRef = builder.scan("EMPS").field("DEPTNO");

        RexLiteral literal1 = (RexLiteral) builder.literal(5);
        RexLiteral literal2 = (RexLiteral) builder.literal(50);
        RexLiteral literal3 = (RexLiteral) builder.literal(20);

        double sensitivity1 = lessThanInteger.calculateSelectivity(literal1, inputRef, columnStatDeptNo);
        Assert.assertEquals(1.0, sensitivity1, CalciteTestUtils.DELTA);

        double sensitivity2 = lessThanInteger.calculateSelectivity(literal2, inputRef, columnStatDeptNo);
        Assert.assertEquals(0.0, sensitivity2, CalciteTestUtils.DELTA);

        double sensitivity3 = lessThanInteger.calculateSelectivity(literal3, inputRef, columnStatDeptNo);
        Assert.assertEquals(0.6666666666666666, sensitivity3, CalciteTestUtils.DELTA);
    }

    @Test
    public void literalLiteralSelectivityTest() {
        RexLiteral a = (RexLiteral) builder.literal(40);
        RexLiteral b = (RexLiteral) builder.literal(50);
        RexLiteral c = (RexLiteral) builder.literal(30);

        double sensitivity = lessThanInteger.calculateSelectivity(a, b);
        Assert.assertEquals(1.0, sensitivity, CalciteTestUtils.DELTA);

        double sensitivity2 = lessThanInteger.calculateSelectivity(a, c);
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

        double sensitivity1 = lessThanInteger.calculateSelectivity(inputRef1, inputRef3, columnStatDeptNo, columnStatEmpNo);
        Assert.assertEquals(1.0, sensitivity1, CalciteTestUtils.DELTA);

        double sensitivity2 = lessThanInteger.calculateSelectivity(inputRef3, inputRef1, columnStatEmpNo, columnStatDeptNo);
        Assert.assertEquals(0.0, sensitivity2, CalciteTestUtils.DELTA);

        double sensitivity3 = lessThanInteger.calculateSelectivity(inputRef1, inputRef2, columnStatDeptNo, columnStatEmpId);
        Assert.assertEquals(0.3333333333333333, sensitivity3, CalciteTestUtils.DELTA);
    }
}
