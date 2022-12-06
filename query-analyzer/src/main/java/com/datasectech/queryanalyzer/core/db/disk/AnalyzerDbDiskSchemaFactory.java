package com.datasectech.queryanalyzer.core.db.disk;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Map;

public class AnalyzerDbDiskSchemaFactory implements SchemaFactory {

    private final static Logger logger = LogManager.getLogger(AnalyzerDbDiskSchemaFactory.class);

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        logger.debug("Creating schema: {}", name);

        String statisticDirName = String.valueOf(operand.getOrDefault("directory", "statistics"));
        File statisticDir = new File(statisticDirName);

        if (!statisticDir.isAbsolute()) {
            String baseDir = String.valueOf(operand.get("baseDirectory"));
            statisticDir = new File(baseDir, statisticDirName);
        }

        return new AnalyzerDbDiskSchema(name, statisticDir);
    }
}
