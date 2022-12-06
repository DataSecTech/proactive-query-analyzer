package com.datasectech.queryanalyzer.core.query.sensitivity.metadata;

import com.datasectech.queryanalyzer.core.query.QueryBuilder;
import com.datasectech.queryanalyzer.core.query.dto.Bucket;
import com.datasectech.queryanalyzer.core.query.dto.ColumnStatistics;
import com.datasectech.queryanalyzer.core.query.dto.Histogram;
import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

public class JdbcMetadataReader extends MetadataReader {

    private final static Logger logger = LogManager.getLogger(JdbcMetadataReader.class);

    protected String schemaName;
    protected CalciteConnection calciteConnection;

    public JdbcMetadataReader(
            String schemaName,
            CalciteConnection calciteConnection,
            Map<String, TableStatistics> schemaStats
    ) {
        super(schemaStats);

        if (schemaName == null || schemaName.isEmpty()) {
            throw new RuntimeException("Schema name is null or empty");
        }

        this.schemaName = schemaName;
        this.calciteConnection = calciteConnection;
    }

    public void calculateTableStatistics() {
        logger.info("Calculating calcite table statistics");

        try {
            initializeTableStatWithColumnTypes();
            readColumnStatistics();

        } catch (SQLException e) {
            logger.error("Failed to build table statistics");
            throw new RuntimeException(e);
        }
    }

    protected void initializeTableStatWithColumnTypes() throws SQLException {

        ResultSet resultSet = calciteConnection.getMetaData()
                .getColumns(null, null, null, null);

        while (resultSet.next()) {
            String schema = resultSet.getString(2);

            if (!schema.equals(schemaName)) {
                continue;
            }

            String table = resultSet.getString(3);
            String column = resultSet.getString(4);
            String dataType = resultSet.getString(6);

            if (!schemaStats.containsKey(table)) {
                schemaStats.put(table, new TableStatistics());
            }

            TableStatistics tableStatistics = schemaStats.get(table);
            tableStatistics.columnStatisticsMap.put(column, new ColumnStatistics(column, dataType));
        }
    }

    private void readColumnStatistics() throws SQLException {

        for (String table : schemaStats.keySet()) {

            TableStatistics tableStatistics = schemaStats.get(table);
            Set<String> columns = tableStatistics.columnStatisticsMap.keySet();

            String sql = generateStatisticsQuery(table, columns);

            try (
                    Statement statement = calciteConnection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql)
            ) {
                tableStatistics.name = table;

                int rowCount = 0;
                while (resultSet.next()) {
                    rowCount = resultSet.getInt(table + "_total_count");

                    for (String column : columns) {
                        ColumnStatistics columnStatistics = tableStatistics.columnStatisticsMap.get(column);

                        columnStatistics.distinct = Integer.parseInt(resultSet.getString(column + "_distinct_count"));
                        columnStatistics.notNull = Integer.parseInt(resultSet.getString(column + "_notnull_count"));
                        columnStatistics.min = resultSet.getString(column + "_min_value");
                        columnStatistics.max = resultSet.getString(column + "_max_value");

                        if (columnStatistics.dataType.equals("VARCHAR") || columnStatistics.dataType.equals("CHAR")) {
                            columnStatistics.histogram = createHistogram(table, column);
                        }
                    }
                }

                tableStatistics.totalRows = rowCount;
            }
        }
    }

    private String generateStatisticsQuery(String table, Set<String> columns) {
        StringBuilder sqlStringBuilder = new StringBuilder("select count(*) as ")
                .append(table)
                .append("_total_count,");

        for (String column : columns) {
            sqlStringBuilder.append(QueryBuilder.createSQL(column));
        }
        int lastIndexOfStringBuilder = sqlStringBuilder.length() - 1;

        if (sqlStringBuilder.charAt(lastIndexOfStringBuilder) == ',') {
            sqlStringBuilder.deleteCharAt(lastIndexOfStringBuilder);
        }

        return sqlStringBuilder.append(" from ").append(table)
                .toString();
    }

    protected Histogram createHistogram(String tableName, String columnName) throws SQLException {
        Histogram histogram = new Histogram("Frequency", tableName + "." + columnName);
        Map<String, Bucket> bucketMap = histogram.bucketMap;

        //TODO - Include dbName in the following query
        String sql = "select " + columnName + " from " + tableName;
        try (
                Statement statement = calciteConnection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            while (resultSet.next()) {
                String colValue = resultSet.getString(1);

                String bucketKey = colValue.isEmpty() ? "" :
                        String.valueOf(colValue.charAt(0)).toLowerCase();

                if (bucketMap.get(bucketKey) == null) {

                    Bucket bucket = new Bucket();

                    bucket.min = colValue;
                    bucket.max = colValue;
                    bucket.noOfItems = 1;

                    bucketMap.put(bucketKey, bucket);
                    continue;
                }

                if (colValue.compareTo(bucketMap.get(bucketKey).min) < 0) {
                    bucketMap.get(bucketKey).min = colValue;
                }

                if (colValue.compareTo(bucketMap.get(bucketKey).max) > 0) {
                    bucketMap.get(bucketKey).max = colValue;
                }

                bucketMap.get(bucketKey).noOfItems++;
            }
        }

        return histogram;
    }
}
