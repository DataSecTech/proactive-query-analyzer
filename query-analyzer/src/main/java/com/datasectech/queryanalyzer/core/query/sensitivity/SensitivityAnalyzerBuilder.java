package com.datasectech.queryanalyzer.core.query.sensitivity;

import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.datasectech.queryanalyzer.core.query.sensitivity.metadata.FixedMetadataReader;
import com.datasectech.queryanalyzer.core.query.sensitivity.metadata.JdbcMetadataReader;
import org.apache.calcite.jdbc.CalciteConnection;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

public class SensitivityAnalyzerBuilder {

    private Writer writer;
    private CalciteMetadataStore metadataStore;

    public SensitivityAnalyzerBuilder writer(Writer writer) {
        this.writer = writer;
        return this;
    }

    public SensitivityAnalyzerBuilder jdbcMetadataReader(
            String schemaName, CalciteConnection calciteConnection, Map<String, String> sensitiveColumns
    ) {

        JdbcMetadataReader metadataReader = new JdbcMetadataReader(schemaName, calciteConnection, new HashMap<>());
        metadataStore = new CalciteMetadataStore(metadataReader, sensitiveColumns);

        return this;
    }

    public SensitivityAnalyzerBuilder fixedMetadataReader(
            Map<String, TableStatistics> schemaStats, Map<String, String> sensitiveColumns
    ) {
        FixedMetadataReader metadataReader = new FixedMetadataReader(schemaStats);
        metadataStore = new CalciteMetadataStore(metadataReader, sensitiveColumns);

        return this;
    }

    public SensitivityAnalyzer build() {
        if (writer == null) {
            writer = new StringWriter();
        }

        if (metadataStore == null) {
            throw new RuntimeException("Need to initialize a metadata store to build SensitivityAnalyzer");
        }

        return new SensitivityAnalyzer(new PrintWriter(writer), metadataStore);
    }
}
