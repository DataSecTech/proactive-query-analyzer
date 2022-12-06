package com.datasectech.queryanalyzer.core.query.writer;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.sql.SqlExplainLevel;

import java.io.PrintWriter;
import java.io.StringWriter;

public class SDLPlanWriterUtils {

    public static String toXML(final RelNode rel) {
        return toXML(rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    }

    public static String toXML(final RelNode rel, SqlExplainLevel detailLevel) {
        if (rel == null) {
            return null;
        }

        final StringWriter sw = new StringWriter();
        final RelWriter planWriter = new RelXmlWriter(new PrintWriter(sw), detailLevel);

        rel.explain(planWriter);

        return sw.toString();
    }

    public static String toPaths(final RelNode rel) {

        if (rel == null) {
            return null;
        }

        final StringWriter sw = new StringWriter();
        final RelWriter pathWriter = new SDLPathWriter(new PrintWriter(sw));

        rel.explain(pathWriter);

        return sw.toString();
    }
}
