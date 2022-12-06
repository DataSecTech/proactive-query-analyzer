package com.datasectech.queryanalyzer.core.query.writer;

import com.datasectech.queryanalyzer.core.query.SDLRelTraverser;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;

public class SDLPathWriter extends SDLRelTraverser {

    public SDLPathWriter(PrintWriter pw) {
        super(pw);
    }

    @Override
    protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
        pw.print(rel);
        printPath();

        extractAndExplainInputs(rel, values);
    }

    private void printPath() {
        if (nodeParentPath.size() == 0) {
            pw.println(" ** ROOT **");
            return;
        }

        String path = nodeParentPath.stream()
                .map(Object::toString)
                .collect(Collectors.joining(" -> "));

        pw.println(" -> " + path);
    }
}
