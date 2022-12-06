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

public class EqualsDateTest {

    protected EqualsDate equalsDate;
    protected static RelBuilder builder;
    protected static ColumnStatistics columnStatDeptNo;

    @BeforeClass
    public static void setBuilder() throws SQLException {
        builder = CalciteTestUtils.getRelBuilder();

        columnStatDeptNo = new ColumnStatistics();

        columnStatDeptNo.name = "JOINEDAT";
        columnStatDeptNo.dataType = "DATE";
        columnStatDeptNo.distinct = 5;
        columnStatDeptNo.min = "1996-08-03";
        columnStatDeptNo.max = "2007-01-01";
    }

    @Before
    public void setUp() {
        equalsDate = new EqualsDate();
    }

    @Test
    public void inputRefLiteralSelectivityTest() {
        RexInputRef inputRef = builder.scan("EMPS").field("JOINEDAT");

        RexLiteral literal1 = (RexLiteral) builder.literal("2002-05-03");
        RexLiteral literal2 = (RexLiteral) builder.literal("1992-01-25");
        RexLiteral literal3 = (RexLiteral) builder.literal("2020-01-01");

        double sensitivity1 = equalsDate.calculateSelectivity(inputRef, literal1, columnStatDeptNo);
        Assert.assertEquals((double) 1 / columnStatDeptNo.distinct, sensitivity1, CalciteTestUtils.DELTA);

        double sensitivity2 = equalsDate.calculateSelectivity(inputRef, literal2, columnStatDeptNo);
        Assert.assertEquals(0.0, sensitivity2, CalciteTestUtils.DELTA);

        double sensitivity3 = equalsDate.calculateSelectivity(inputRef, literal3, columnStatDeptNo);
        Assert.assertEquals(0.0, sensitivity3, CalciteTestUtils.DELTA);
    }


    @Test
    public void literalInputRefSelectivityTest() {
        RexInputRef inputRef = builder.scan("EMPS").field("JOINEDAT");

        RexLiteral literal1 = (RexLiteral) builder.literal("2002-05-03");
        RexLiteral literal2 = (RexLiteral) builder.literal("1992-01-25");
        RexLiteral literal3 = (RexLiteral) builder.literal("2020-01-01");

        double sensitivity1 = equalsDate.calculateSelectivity(literal1, inputRef, columnStatDeptNo);
        Assert.assertEquals((double) 1 / columnStatDeptNo.distinct, sensitivity1, CalciteTestUtils.DELTA);

        double sensitivity2 = equalsDate.calculateSelectivity(literal2, inputRef, columnStatDeptNo);
        Assert.assertEquals(0.0, sensitivity2, CalciteTestUtils.DELTA);

        double sensitivity3 = equalsDate.calculateSelectivity(literal3, inputRef, columnStatDeptNo);
        Assert.assertEquals(0.0, sensitivity3, CalciteTestUtils.DELTA);
    }

    @Test
    public void literalLiteralSelectivityTest() {
        RexLiteral a = (RexLiteral) builder.literal("2002-05-03");
        RexLiteral b = (RexLiteral) builder.literal("2002-05-03");
        RexLiteral c = (RexLiteral) builder.literal("1992-01-25");

        double sensitivity = equalsDate.calculateSelectivity(a, b);
        Assert.assertEquals(1.0, sensitivity, CalciteTestUtils.DELTA);

        double sensitivity2 = equalsDate.calculateSelectivity(a, c);
        Assert.assertEquals(0.0, sensitivity2, CalciteTestUtils.DELTA);
    }

    @Test
    public void inputRefInputRefSelectivityTest() {
        RexInputRef inputRef1 = builder.scan("EMPS").field("JOINEDAT");
        RexInputRef inputRef2 = builder.scan("PERSONS").field("BIRTHDATE");

        ColumnStatistics columnStatBirthDate = new ColumnStatistics();
        columnStatBirthDate.name = "BIRTHDATE";
        columnStatBirthDate.dataType = "DATE";
        columnStatBirthDate.distinct = 4;
        columnStatBirthDate.min = "1980-03-09";
        columnStatBirthDate.max = "2001-01-01";

        double sensitivity = equalsDate.calculateSelectivity(inputRef1, inputRef2, columnStatDeptNo, columnStatBirthDate);
        Assert.assertEquals(0.4238821995551806, sensitivity, CalciteTestUtils.DELTA);
    }
}
