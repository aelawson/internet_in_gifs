package org.aelawson.util;

import org.apache.flink.api.java.functions.KeySelector;

import org.aelawson.util.SemanticSummary;

public class KeyBySubject implements KeySelector<SemanticSummary, String> {
    public static final long serialVersionUID = 1L;

    @Override
    public String getKey(SemanticSummary summary) {
        return summary.subject;
    }
}