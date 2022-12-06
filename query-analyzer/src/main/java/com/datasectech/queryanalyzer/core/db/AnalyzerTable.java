package com.datasectech.queryanalyzer.core.db;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class AnalyzerTable extends AbstractTable implements ScannableTable {

    protected final String tableName;
    protected final List<String> names;
    protected final List<SqlTypeName> types;
    protected List<RelDataType> relDataType;

    public AnalyzerTable(String tableName, List<String> names, List<SqlTypeName> types) {
        this.tableName = tableName;
        this.names = names;
        this.types = types;
    }

    public List<RelDataType> getRelDataType(RelDataTypeFactory typeFactory) {
        if (relDataType == null) {

            relDataType = new ArrayList<>();

            for (int i = 0; i < types.size(); i++) {
                SqlTypeName type = types.get(i);
                if (type == null) {
                    throw new RuntimeException("SqlDataType is null at index " + i + " for table " + tableName);
                }

                relDataType.add(typeFactory.createSqlType(type));
            }
        }
        return relDataType;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(Pair.zip(names, getRelDataType(typeFactory)));
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return null;
    }
}
