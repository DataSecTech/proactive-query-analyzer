package com.datasectech.queryanalyzer.core.query.sensitivity.filters.datatypes;

import com.datasectech.queryanalyzer.core.query.dto.Bucket;
import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import com.datasectech.queryanalyzer.core.query.dto.Histogram;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class LessThanStringTest {

    protected LessThanString lessThanString;
    protected static RelBuilder builder;
    protected static ColumnStatistics columnStatName;

    @BeforeClass
    public static void setBuilder() throws SQLException {
        builder = CalciteTestUtils.getRelBuilder();

        columnStatName = new ColumnStatistics();
        columnStatName.name = "NAME";
        columnStatName.dataType = "VARCHAR";
        columnStatName.distinct = 5;
        columnStatName.notNull = 5;
        columnStatName.min = "Alice";
        columnStatName.max = "Wilma";

        Histogram histogram = new Histogram("Frequency", "EMPS.NAME");

        Bucket bucketA = new Bucket();
        bucketA.min = "Alice";
        bucketA.max = "Alice";
        bucketA.noOfItems = 1;

        Bucket bucketE = new Bucket();
        bucketE.min = "Eric";
        bucketE.max = "Eric";
        bucketE.noOfItems = 1;

        Bucket bucketF = new Bucket();
        bucketF.min = "Fred";
        bucketF.max = "Fred";
        bucketF.noOfItems = 1;

        Bucket bucketJ = new Bucket();
        bucketJ.min = "John";
        bucketJ.max = "John";
        bucketJ.noOfItems = 1;

        Bucket bucketW = new Bucket();
        bucketW.min = "Wilma";
        bucketW.max = "Wilma";
        bucketW.noOfItems = 1;

        Map<String, Bucket> bucketMap = new HashMap<>();
        bucketMap.put("a", bucketA);
        bucketMap.put("e", bucketE);
        bucketMap.put("f", bucketF);
        bucketMap.put("j", bucketJ);
        bucketMap.put("w", bucketW);

        histogram.bucketMap = bucketMap;

        columnStatName.histogram = histogram;
    }

    @Before
    public void setUp() {
        lessThanString = new LessThanString();
    }

    @Test
    public void inputRefLiteralSelectivityTest() {
        RexInputRef inputRef = builder.scan("EMPS").field("NAME");

        RexLiteral literal1 = (RexLiteral) builder.literal("Anna");
        RexLiteral literal2 = (RexLiteral) builder.literal("Zoe");
        RexLiteral literal3 = (RexLiteral) builder.literal("Grey");

        double sensitivity1 = lessThanString.calculateSelectivity(inputRef, literal1, columnStatName);
        Assert.assertEquals(0.0, sensitivity1, CalciteTestUtils.DELTA);

        double sensitivity2 = lessThanString.calculateSelectivity(inputRef, literal2, columnStatName);
        Assert.assertEquals(1.0, sensitivity2, CalciteTestUtils.DELTA);

        double sensitivity3 = lessThanString.calculateSelectivity(inputRef, literal3, columnStatName);
        Assert.assertEquals(0.6, sensitivity3, CalciteTestUtils.DELTA);
    }

    @Test
    public void literalInputRefSelectivityTest() {
        RexInputRef inputRef = builder.scan("EMPS").field("NAME");

        RexLiteral literal1 = (RexLiteral) builder.literal("Anna");
        RexLiteral literal2 = (RexLiteral) builder.literal("Zoe");
        RexLiteral literal3 = (RexLiteral) builder.literal("Grey");

        double sensitivity1 = lessThanString.calculateSelectivity(literal1, inputRef, columnStatName);
        Assert.assertEquals(1.0, sensitivity1, CalciteTestUtils.DELTA);

        double sensitivity2 = lessThanString.calculateSelectivity(literal2, inputRef, columnStatName);
        Assert.assertEquals(0.0, sensitivity2, CalciteTestUtils.DELTA);

        double sensitivity3 = lessThanString.calculateSelectivity(literal3, inputRef, columnStatName);
        Assert.assertEquals(0.4, sensitivity3, CalciteTestUtils.DELTA);
    }

    @Test
    public void literalLiteralSelectivityTest() {
        RexLiteral a = (RexLiteral) builder.literal("Grey");
        RexLiteral b = (RexLiteral) builder.literal("Zoe");
        RexLiteral c = (RexLiteral) builder.literal("Anna");

        double sensitivity = lessThanString.calculateSelectivity(a, b);
        Assert.assertEquals(1.0, sensitivity, CalciteTestUtils.DELTA);

        double sensitivity2 = lessThanString.calculateSelectivity(a, c);
        Assert.assertEquals(0.0, sensitivity2, CalciteTestUtils.DELTA);
    }

    @Test
    public void inputRefInputRefSelectivityTest() {
        RexInputRef inputRef1 = builder.scan("EMPS").field("NAME");
        RexInputRef inputRef2 = builder.scan("EMPS").field("CITY");

        ColumnStatistics columnStatCity = new ColumnStatistics();
        columnStatCity.name = "CITY";
        columnStatCity.dataType = "VARCHAR";
        columnStatCity.distinct = 3;
        columnStatCity.notNull = 5;
        columnStatCity.min = "";
        columnStatCity.max = "Vancouver";


        double sensitivity1 = lessThanString.calculateSelectivity(inputRef1, inputRef2, columnStatName, columnStatCity);
        Assert.assertEquals(0.3333333333333333, sensitivity1, CalciteTestUtils.DELTA);
    }
}
