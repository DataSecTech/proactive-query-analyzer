package com.datasectech.queryanalyzer.core.query;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.util.Pair;

import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class SDLRelTraverser extends RelWriterImpl {

    protected final Deque<RelNode> nodeParentPath;

    public SDLRelTraverser(PrintWriter pw) {
        super(pw);
        this.nodeParentPath = new ArrayDeque<>();
    }

    public List<RelNode> extractAndExplainInputs(RelNode rel, List<Pair<String, Object>> values) {

        final List<RelNode> inputs = extractInputs(values);

        nodeParentPath.addFirst(rel);

        for (RelNode input : inputs) {
            input.explain(this);
        }

        nodeParentPath.removeFirst();

        return inputs;
    }

    public static Object extractValue(String propertyName, List<Pair<String, Object>> values) {

        for (Pair<String, Object> pair : values) {
            if (propertyName.equals(pair.left)) {
                return pair.right;
            }
        }

        throw new RuntimeException("Property " + propertyName + " not found in values.");
    }

    public static List<RelNode> extractInputs(List<Pair<String, Object>> values) {

        final List<RelNode> inputs = new ArrayList<>();
        for (Pair<String, Object> pair : values) {
            if (pair.right instanceof RelNode) {
                inputs.add((RelNode) pair.right);
            }
        }

        return inputs;
    }
}
