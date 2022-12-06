package com.datasectech.queryanalyzer.core.query;

import com.mitchtalmadge.asciidata.table.ASCIITable;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class OutputPrinter {

    private final static Logger logger = LogManager.getLogger(OutputPrinter.class);

    public enum OutputType {
        PARSED_SQL,
        PLAN_TEXT,
        PATH_TEXT,
        PLAN_XML;

        public String extension() {
            String[] parts = toString().split("_");
            return parts[parts.length - 1].toLowerCase();
        }

        public String relativePath(String queryId) {
            return String.format("%s/%s.%s", toString().toLowerCase(), queryId, extension());
        }
    }

    protected File outputBase;

    public OutputPrinter(File outputBase) {
        this.outputBase = outputBase;
    }

    public void saveContent(OutputType type, String queryId, String content, boolean overwrite) {

        File outputFile = new File(outputBase, type.relativePath(queryId));
        outputFile.getParentFile().mkdirs();

        try {

            if (outputFile.exists() && !overwrite) {
                logger.debug("File exists, not overwriting {}", outputFile);
            } else {
                logger.info("Writing {}", outputFile);
                FileUtils.write(outputFile, content, StandardCharsets.UTF_8);
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to write file: " + outputFile, e);
        }
    }

    public static void printResultSet(ResultSet rs) throws SQLException {

        ResultSetMetaData rsMetaData = rs.getMetaData();
        int columnsNumber = rsMetaData.getColumnCount();

        String[] header = new String[columnsNumber];

        for (int i = 1; i <= columnsNumber; i++) {
            header[i - 1] = rsMetaData.getColumnName(i);
        }

        List<String[]> dataList = new ArrayList<>();

        while (rs.next()) {

            String[] row = new String[columnsNumber];
            for (int i = 1; i <= columnsNumber; i++) {
                String columnValue = rs.getString(i);
                row[i - 1] = columnValue == null ? " " : columnValue;
            }

            dataList.add(row);
        }

        String[][] data = new String[dataList.size()][];

        for (int i = 0; i < dataList.size(); i++) {
            data[i] = dataList.get(i);
        }

        System.out.println(ASCIITable.fromData(header, data));
    }
}
