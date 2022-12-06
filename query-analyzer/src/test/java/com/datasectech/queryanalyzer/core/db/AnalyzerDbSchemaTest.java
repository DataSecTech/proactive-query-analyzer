package com.datasectech.queryanalyzer.core.db;

import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class AnalyzerDbSchemaTest {

    protected AnalyzerDbSchema analyzerDbSchema;

    @Before
    public void setUp() throws Exception {
        analyzerDbSchema = new AnalyzerDbSchema("TEST_SCHEMA") {
            @Override
            public Map<String, TableStatistics> readSchemaStatistics() {
                return null;
            }
        };
    }

    @Test
    public void findSqlTypeName() {

        Assert.assertEquals(SqlTypeName.INTEGER, analyzerDbSchema.findSqlTypeName("int"));
        Assert.assertEquals(SqlTypeName.INTEGER, analyzerDbSchema.findSqlTypeName("INT"));
        Assert.assertEquals(SqlTypeName.INTEGER, analyzerDbSchema.findSqlTypeName("integer"));
        Assert.assertEquals(SqlTypeName.INTEGER, analyzerDbSchema.findSqlTypeName("INTEGER"));

        Assert.assertEquals(SqlTypeName.VARCHAR, analyzerDbSchema.findSqlTypeName("string"));
        Assert.assertEquals(SqlTypeName.VARCHAR, analyzerDbSchema.findSqlTypeName("STRING"));
        Assert.assertEquals(SqlTypeName.VARCHAR, analyzerDbSchema.findSqlTypeName("varchar"));
        Assert.assertEquals(SqlTypeName.VARCHAR, analyzerDbSchema.findSqlTypeName("VARCHAR"));

        Assert.assertEquals(SqlTypeName.CHAR, analyzerDbSchema.findSqlTypeName("char"));
        Assert.assertEquals(SqlTypeName.CHAR, analyzerDbSchema.findSqlTypeName("CHAR"));
    }

    @Test(expected = RuntimeException.class)
    public void findSqlTypeExceptionNull() {
        analyzerDbSchema.findSqlTypeName(null);
    }

    @Test(expected = RuntimeException.class)
    public void findSqlTypeExceptionInvalid() {
        analyzerDbSchema.findSqlTypeName("Invalid");
    }
}