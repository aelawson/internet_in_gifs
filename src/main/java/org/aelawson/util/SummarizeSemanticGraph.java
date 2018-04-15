package org.aelawson.util;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import edu.stanford.nlp.semgraph.SemanticGraph;

import org.aelawson.util.SemanticSummary;

public class SummarizeSemanticGraph implements FlatMapFunction<SemanticGraph, SemanticSummary> {
    public static final long serialVersionUID = 1L;

    @Override
    public void flatMap(SemanticGraph value, Collector<SemanticSummary> out) {
        value.edgeListSorted().stream()
            .filter(e -> "nsubj".equals(e.getRelation().getShortName()))
            .map(e -> e.getTarget().toString())
            .forEach(s -> out.collect(new SemanticSummary(s)));
    }
};
